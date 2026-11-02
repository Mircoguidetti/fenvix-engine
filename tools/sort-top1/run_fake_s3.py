import sys, time
import os; sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ctl-tests"))
import fake_aws
orig = fake_aws._Server
class P(orig):
    def __init__(self, addr, h):
        super().__init__(("127.0.0.1", 9000), h)
fake_aws._Server = P
r = fake_aws.Registry(); r.add("minioadmin", "minioadmin", None, 0, 1e12)
s = fake_aws.FakeS3(r)
print(s.url, flush=True)
while True: time.sleep(3600)
