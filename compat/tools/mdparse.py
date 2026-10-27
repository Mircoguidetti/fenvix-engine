import re
LINK = re.compile(r'^\|\s*\[`?(.+?)`?\]\((https://www\.mongodb\.com/docs/[^)\s]+)\)\s*\|(.*)\|\s*$')
def entries(text):
    """Yield (h2, h3, name, url, desc) for each table row whose first cell is a docs link."""
    h2 = h3 = ""
    for line in text.splitlines():
        if line.startswith("## "): h2, h3 = line[3:].strip(), ""
        elif line.startswith("### "): h3 = line[4:].strip()
        m = LINK.match(line)
        if m:
            desc = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', m.group(3)).replace('`','').strip()
            yield h2, h3, m.group(1).strip(), m.group(2), desc
