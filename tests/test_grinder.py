import unittest

from grinder.grinder import Grinder

class TestGrinder(unittest.TestCase):

    def writeTestFile(self, path):
        f = open(path, 'arw')
        f.write('TEST NOT USED: %s' % path)
        f.close()
        
    def test_isPkgShortNewer(self):
        certpath = '/tmp/grinder-test-cert.xml'
        systemidpath = '/tmp/grinder-test-systemid'
        self.writeTestFile(certpath)
        self.writeTestFile(systemidpath)
        

        g = Grinder('http://example.com', 'username', 'password', 
            certpath, systemidpath, 1, 0)

        assert(False)


if __name__ == '__main__':
    unittest.main()
