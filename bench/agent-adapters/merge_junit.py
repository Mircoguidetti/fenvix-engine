"""Merge several JUnit XML files into one <testsuites> file: merge_junit.py OUT IN [IN ...] (missing inputs are skipped)."""
import os
import sys
import xml.etree.ElementTree as ET

out, ins = sys.argv[1], sys.argv[2:]
root = ET.Element("testsuites")
for f in ins:
    if not os.path.exists(f):
        continue
    r = ET.parse(f).getroot()
    for s in ([r] if r.tag == "testsuite" else list(r)):
        root.append(s)
ET.ElementTree(root).write(out)
