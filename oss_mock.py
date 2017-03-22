#coding=utf8
import os
import time
import httplib
import md5
import urlparse
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import simplejson as json
import threading
import ConfigParser
import logging
import logging.handlers
import random

oss_ip=''
oss_port=''
#udf_map = { 'udf_1' : { 'ip': 'x.x.x.x', 'status': 'running/fault' } }
udf_map = {}

class MyHTTPRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        logging.info("%s %s" % (self.address_string(),format%args))
        return

    def get_local_file(self, url_path):
        m = md5.new()
        m.update(url_path)
        return '/tmp/oss_mock/%s' % m.hexdigest()

    def parse_udf_para(self, process_para):
        str = ''
        if process_para.startswith('udf/'):
            str = process_para[4:len(process_para)]
        else:
            str = process_para

        udf_name = ''
        udf_para = ''
        pos = str.find(',')
        if pos > 0:
            udf_name = str[0:pos]
            udf_para = str[pos+1:len(str)]
        elif pos == -1:
            udf_name = str
        else:
            return ('', '')

        global udf_map
        if udf_map.has_key(udf_name):
            return (udf_name, udf_para)
        return ('', '')

    def random_string(self, len):
        char_set = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G']
        list = []
        for i in range(0, len):
            list.append(random.choice(char_set))
        return "".join(list)

    def do_udf_request(self, url_path, process_para):
        file_name = self.get_local_file(url_path)
        if not os.path.exists(file_name):
            self.complete_request(404, 'NoSuchKey', 'The specified key does not exist.')
            return

        (udf_name, udf_para) = self.parse_udf_para(process_para)
        if udf_name == '':
            self.complete_request(404, 'UdfFailed', 'The specified udf does not exist.')
            return

        req = {}
        req['bucket'] = self.bucket
        req['filesize'] = os.path.getsize(file_name)
        req['object'] = 'udf'
        req['owner'] = 'TestUser'
        req['reqId'] = self.random_string(24)
        req['reqParams'] = ''
        req['resUrl'] = 'http://' + self.headers['Host'] + url_path
        req['udfName'] = udf_name
        req['udfParam'] = udf_para
        req['version'] = '1.0'

        if self.headers.has_key('Range'):
            req_params = {}
            req_params['range'] = self.headers['Range']
            req['reqParams'] = json.dumps(req_params)
        else:
            req['reqParams'] = '{}'

        
        global udf_map
        if not udf_map.has_key(udf_name):
            self.complete_request(404, 'UdfFailed', udf_name + ' does not exist.')
            return

        if udf_map[udf_name]['status'] != 'running':
            self.complete_request(404, 'UdfFailed', udf_name + ' is Fault.')
            return

        req_body = json.dumps(req)
        logging.info('udf req: %s' % (req_body))
        headers = {}
        try:
            conn = httplib.HTTPConnection('%s:9000' % (udf_map[udf_name]['ip']))
            conn.request("POST", "/udf", req_body, headers)
            response = conn.getresponse()
            content = response.read()
            conn.close()

            self.send_response(response.status)
            self.end_headers()
            self.wfile.write(content)
        except:
            self.complete_request(404, 'UdfFailed', udf_name + ' is not available') 

        return

    def check_host(self):
        self.host = ''
        self.bucket = ''
        if self.headers['Host'] == '' or len(self.headers['Host'].split('.')) < 4:
            self.complete_request(400, 'BadRequest', 'Invalid Host')
            return False

        self.host = self.headers['Host']
        self.bucket = self.headers['Host'].split('.')[0]
        return True

    def check_para(self):
        if not self.check_host():
            return False

        content_length = self.headers['content-length']
        if '' == content_length or 0 == int(content_length):
            self.complete_request(400, 'BadRequest', 'Invalid Content-Length')
            return False

        return True

    def complete_request(self, status, code, message):
        xml = ''
        xml += '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml += '<Error>\n'
        xml += '  <Code>' + code + '</Code>\n'
        xml += '  <Message>' + message + '</Message>\n'
        xml += '  <HostId>' + self.host + '</HostId>\n'
        xml += '  <BucketName>' + self.bucket + '</BucketName>\n'
        xml += '</Error>\n'

        self.send_response(status)
        self.send_header('Content-Type', 'application/xml')
        self.send_header('Content-Length', str(len(xml)))
        self.end_headers()
        self.wfile.write(xml)

    def parse_range(self, range_str, file_size):
        #bytes=100-300
        #bytes=100-
        #bytes=-300
        if not range_str.startswith('bytes='):
            return False, 0, file_size

        range = range_str[len('bytes='):len(range_str)]
        start_str = range.split('-')[0]
        end_str = range.split('-')[1]

        if start_str == '' and end_str == '' and len(range.split('-')) != 2:
            return True, 0, file_size - 1
        elif start_str == '':
            return True, file_size - int(end_str), file_size - 1
        elif end_str == '':
            return True, int(start_str), file_size - 1
        else:
            return True, int(start_str), int(end_str)

    def do_GET(self):
        logging.info('GET path: %s headers: %s' % (self.path, self.headers))

        if not self.check_host():
            return

        host = self.headers['Host']
        parsed_result = urlparse.urlparse(self.path)
        url_path = parsed_result.path

        # check udf para
        process_para = ''
        if urlparse.parse_qs(parsed_result.query).has_key('x-oss-process'):
            process_para = urlparse.parse_qs(parsed_result.query)['x-oss-process'][0]
        elif self.headers.has_key('x-oss-process'):
            process_para = self.headers['x-oss-process']

        if process_para != '':
            # do udf
            self.do_udf_request(url_path, process_para.strip())
            return
    
        file_name = self.get_local_file(url_path)
        if not os.path.exists(file_name):
            self.complete_request(404, 'NoSuchKey', 'The specified key does not exist.')
            return

        file_size = os.path.getsize(file_name)
        valid_range = False
        range_start = 0
        range_end = file_size - 1
        if self.headers.has_key('Range'):
            try:
                valid_range, range_start, range_end = self.parse_range(self.headers['Range'], file_size)
            except:
                pass

        logging.info('GET' + file_name)
        with open(file_name, 'rb') as fileobj:
            self.send_response(200)
            if valid_range:
                self.send_header('Content-Range', 'bytes %ld-%ld/%ld' % (range_start, range_end, file_size))
            self.send_header('Content-Length', '%ld' % (range_end + 1 - range_start))
            self.end_headers()

            fileobj.seek(range_start)
            remaining_bytes = range_end + 1 - range_start
            while remaining_bytes > 0:
                read_size = remaining_bytes if remaining_bytes <= 1024 * 1024 else 1024 * 1024
                remaining_bytes = remaining_bytes - read_size
                content = fileobj.read(read_size)
                self.wfile.write(content)
                self.wfile.flush()
            return

        self.complete_request(500, 'InternalError', 'UnknowError')
        return

    def do_PUT(self):
        logging.info('PUT path: %s headers: %s' % (self.path, self.headers))

        if not self.check_para():
            return

        parsed_result = urlparse.urlparse(self.path)
        url_path = parsed_result.path
        file_name = self.get_local_file(url_path)
        content_length = self.headers.getheader('Content-Length')
        remaining_bytes = int(content_length)
        if not os.path.exists('/tmp/oss_mock'):
            os.mkdir('/tmp/oss_mock')
        logging.info('PUT ' + file_name)
        with open(file_name, 'wb') as fileobj:
            while remaining_bytes > 0:
                read_size = remaining_bytes if remaining_bytes < 1024 * 1024 else 1024 * 1024
                remaining_bytes = remaining_bytes - read_size
                content = self.rfile.read(read_size)
                fileobj.write(content)
            self.send_response(200)
            self.end_headers()
            return

        self.complete_request(500, 'InternalError', 'UnknowError')
        return

class ThreadingMixIn:
    def process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
            self.shutdown_request(request)
        except:
            self.handle_error(request, client_address)
            self.shutdown_request(request)

    def process_request(self, request, client_address):
        t = threading.Thread(target = self.process_request_thread,
                             args = (request, client_address))
        t.daemon = False
        t.start()

class MyHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, host, port):
        HTTPServer.__init__(self, (host, port), MyHTTPRequestHandler)

class UdfHealthyCheck():
    def run(self):
        t = threading.Thread(target = self.check_thread)
        t.setDaemon(True)
        t.start()

    def check_thread(self):
        while True:
            global udf_map
            for k in udf_map.keys():
                ip = udf_map[k]['ip']
                try:
                    conn = httplib.HTTPConnection('%s:9000' % (ip))
                    headers = {}
                    conn.request("GET", "/CheckHealthy", '', headers)
                    response = conn.getresponse()
                    conn.close()
                    if response.status == 200:
                        udf_map[k]['status'] = 'running'
                    else:
                        udf_map[k]['status'] = 'fault'
                except:
                    udf_map[k]['status'] = 'Fault'

                logging.info('udf: %s status: %s' % (k, udf_map[k]['status']))
            time.sleep(10)

def get_config(section, name):
    cfg_fn = os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/oss_mock.cfg")
    parser = ConfigParser.ConfigParser()
    parser.read(cfg_fn)
    return parser.get(section, name)

def init_config():
    udfs = get_config('UDF', 'udf_list')
    udf_list = udfs.split(',')
    global udf_name
    for x in udf_list:
        info = x.strip()
        if len(info.split(':')) == 2:
            udf_name = info.split(':')[0].strip()
            udf_ip = info.split(':')[1].strip()
            udf_map[udf_name] = { 'ip': udf_ip, 'status': 'Fault' }

    global oss_ip
    global oss_port
    oss_ip = get_config('OSS', 'oss_ip').strip()
    oss_port = get_config('OSS', 'oss_port').strip()

def init_logger(filename, loglevel = logging.INFO, fmt = None):
    logger = logging.getLogger('')
    logger.setLevel(loglevel)

    if not fmt:
        fmt = '[%(asctime)-15s] [%(levelname)s] [%(filename)s:%(lineno)d] [%(thread)d] %(message)s'
    handler = logging.handlers.RotatingFileHandler(filename, maxBytes = 1024*1024*100, backupCount = 5)
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logging.info('init ok')

if __name__ == '__main__':
    # init logger
    init_logger('oss_mock.log')

    # init config
    init_config()

    # start check udf
    udf_checker = UdfHealthyCheck()
    udf_checker.run()

    # start oss mock server
    server = MyHTTPServer(oss_ip, 9011)
    print 'Starting oss'
    server.serve_forever()

