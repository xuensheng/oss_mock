#coding=utf8
import os
import httplib
import urllib2
import md5
import urlparse
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import simplejson as json
import threading
import time
import random

class MyHTTPRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def check_para(self):
        parsed_result = urlparse.urlparse(self.path)
        url_path = parsed_result.path
        if url_path != '/udf':
            self.complete_request(400, 'BadRequest', 'Invalid url')
            return False

        content_length = self.headers['Content-Length']
        if '' == content_length or 0 == int(content_length):
            self.complete_request(400, 'BadRequest', 'Invalid Content-Length')
            return False

        return True

    def do_GET(self):
        parsed_result = urlparse.urlparse(self.path)
        url_path = parsed_result.path
        if url_path != '/CheckHealthy':
            self.complete_request(400, 'BadRequest', 'Invalid url')
            return False

        self.send_response(200)
        self.end_headers()
        return

    def do_POST(self):
        if not self.check_para():
            return

        # get the request
        content_length = self.headers['Content-Length']
        content = self.rfile.read(int(content_length))
        udf_para = json.loads(content)

        # get the object from oss
        url = udf_para['resUrl']
        try:
            req = urllib2.Request(url)
            response = urllib2.urlopen(req)
            content = response.read()
        except urllib2.URLError as e:
            print 'url error'
        except urllib2.HTTPError as e:
            self.complete_request(self, e.code, e.reason, e.read())

        # process the object

        # send response
        self.send_response(200)
        self.end_headers()
        self.wfile.write('UDF OK')

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

class MyHTTPServer(HTTPServer):
    def __init__(self, host, port):
        HTTPServer.__init__(self, (host, port), MyHTTPRequestHandler)


class Udf():
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
   
    def start(self):
        """
        start the udf server on a new thread
        """
        self.server = MyHTTPServer(self.ip, int(self.port))
        self.server_died = threading.Event()
        self.server_thread = threading.Thread(
                target=self.run_thread)
        self.server_thread.setDaemon(True)
        self.server_thread.start()

    def run_thread(self):
        self.server.serve_forever()
        print 'exiting..'
        self.server_died.set()

    def stop(self):
        if not self.server_thread:
            return

        self.server.shutdown()

        # wait for thread to die for a bit, then give up raising an exception.
        if not self.server_died.wait(5):
            raise ValueError("couldn't kill udf server")


def RandomString(len):
    char_set = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g']
    list = []
    for i in range(0, len):
        list.append(random.choice(char_set))
    return "".join(list)

def get_msg(res):
    msg = "***************************OutPut Response**********************\n"
    msg += "header: %s\n" % res.getheaders()
    msg += "body: %s\n" % res.read(500)
    msg += "status: %s\n" % res.status
    msg += "reason: %s\n" % res.reason
    msg += "***************************OutPut Response**********************\n"
    return msg
        
def assert_equal(res, a, b):
    if a != b:
        raise Exception("%s\n%s != %s"% (get_msg(res), a, b))

def assert_true(res, cond):
    if not cond:
        raise Exception(get_msg(res))

class TestOss():
    def __init__(self, oss_host, oss_port):
        self.oss_host = oss_host
        self.oss_port = oss_port

    def setup(self):
        self.bucket = 'test'
        self.udf = Udf('0.0.0.0', '9000')
        self.udf.start()
        time.sleep(10)

    def test_put_object(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        conn.request("PUT", self.object, RandomString(1000), headers)
        response = conn.getresponse()
        conn.close()
        assert_true(response, response.status == 200)

    def test_get_normal(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(1000)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        conn.close()

        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), content)

    def test_get_big_normal(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(10*1024*1024)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        conn.close()

        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), content)

    def test_range_get(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(1000)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        headers['Range'] = 'bytes=2-300'
        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.getheader('content-range'), 'bytes 2-300/%d' % (len(content)))
        assert_equal(response, response.read(), content[2:301])
        
        headers['Range'] = 'bytes=2-'
        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.getheader('content-range'), 'bytes 2-999/%d' % (len(content)))
        assert_equal(response, response.read(), content[2:1000])
        
        headers['Range'] = 'bytes=-300'
        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.getheader('content-range'), 'bytes 700-999/%d' % (len(content)))
        assert_equal(response, response.read(), content[700:1000])

        conn.close()
    def test_get_object_not_exist(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        conn.close()

        assert_true(response, response.status == 404)

    def test_get_object_udf_normal(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(1000)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        conn.request("GET", '/' + self.object + '?x-oss-process=udf/udf_name_1,k1,k2', '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), 'UDF OK')
        
        conn.request("GET", '/' + self.object + '?x-oss-process=udf_name_1,k1,k2', '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), 'UDF OK')
        
        headers['x-oss-process'] = 'udf/udf_name_1,k1,k2'
        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), 'UDF OK')

        headers['x-oss-process'] = 'udf_name_1,k1,k2'
        conn.request("GET", '/' + self.object, '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), 'UDF OK')

        conn.close()

    def test_get_object_udf_normal(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(1000)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        conn.request("GET", '/' + self.object + '?x-oss-process=udf/udf_name_1,k1,k2', '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), 'UDF OK')

        conn.close()
 
    def test_range_get_object_udf(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(1000)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        headers['Range'] = 'bytes=2-300'
        conn.request("GET", '/' + self.object + '?x-oss-process=udf/udf_name_1,k1,k2', '', headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)
        assert_equal(response, response.read(), 'UDF OK')

        conn.close()
     
    def test_get_object_udf_not_exist(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(1000)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        conn.request("GET", '/' + self.object + '?x-oss-process=udf/udf_name_3,k1,k2', '', headers)
        response = conn.getresponse()
        conn.close()

        assert_true(response, response.status == 404)

    def test_get_object_udf_not_healthy(self):
        self.object = RandomString(10) 
        conn = httplib.HTTPConnection('%s:%s' % (self.oss_host, self.oss_port))
        headers = {}
        content = RandomString(1000)
        conn.request("PUT", '/' + self.object, content, headers)
        response = conn.getresponse()
        assert_true(response, response.status == 200)

        conn.request("GET", '/' + self.object + '?x-oss-process=udf/udf_name_2,k1,k2', '', headers)
        response = conn.getresponse()
        conn.close()

        assert_true(response, response.status == 404)

    def is_test_method(self, function):
        if function.startswith('test_'):
            return True
        else:
            return False

    def run_test(self):
        method_list = filter(self.is_test_method, dir(self))
        for method in method_list:
            print 'Run ' + method
            eval('self.' + method)()
            print 'Case %-50s... Passed' % method


if __name__ == '__main__':
    test = TestOss('test.oss.aliyun.com', '9011')
    test.setup()
    test.run_test()

