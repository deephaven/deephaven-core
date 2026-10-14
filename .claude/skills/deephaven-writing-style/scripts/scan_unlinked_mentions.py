"""Lists UNLINKED mentions under the 'every mention of a method or feature is a link' rule.
Skips fenced code, headings, front matter, link text/targets (inline, multiline, and reference-style), URLs. Prints every bare occurrence
(not just the first per paragraph). The reader decides which are columns, user code, JDK names,
parameters, or literal values (those are not linked).
Fences: backtick and tilde fences are tracked by delimiter and length, inside indentation and
blockquotes. Run `python3 -I` on the file."""
import re,sys
PLAIN=[r"\bBarriers?\b",r"\bformula strings?\b",r"\bserial execution\b",r"\bpredicate pushdown\b",r"\bpartitioned tables?\b",r"\bConcurrencyControl\b",r"\bSelectables?\b",r"\bFilters?\b(?= objects?\b| instances?\b)"]
FENCE=re.compile(r"^[ \t>]*(`{3,}|~{3,})(.*)$")
def mask_links(text):
    """Blank inline links (also across lines), reference-style links, and reference definitions, keeping line numbers."""
    keep=lambda m: "\n"*m.group(0).count("\n")
    text=re.sub(r"\[[^\]]*\]\([^)]*\)",keep,text)            # inline links, text and destination
    text=re.sub(r"\[[^\]]*\]\[[^\]]*\]",keep,text)           # reference links [text][ref] and [text][]
    text=re.sub(r"(?m)^[ \t>]*\[[^\]]+\]:[ \t]*\S.*$",keep,text)  # reference definitions [ref]: url
    return text
def scan(path):
    out=[]; lines=mask_links(open(path,encoding="utf-8").read()).split("\n"); fence=None; fm=False
    for i,l in enumerate(lines,1):
        if i==1 and l.strip()=="---": fm=True; continue
        if fm:
            if l.strip()=="---": fm=False
            continue
        m=FENCE.match(l)
        if fence is None:
            if m and not (m.group(1)[0]=="`" and "`" in m.group(2)):
                fence=(m.group(1)[0],len(m.group(1))); continue
        else:
            # a closing fence uses the same character, is at least as long, and has no info string
            if m and m.group(1)[0]==fence[0] and len(m.group(1))>=fence[1] and not m.group(2).strip(): fence=None
            continue
        if l.lstrip(" \t>").startswith("#"): continue
        t=re.sub(r"https?://\S+","",l)
        for mm in re.finditer(r"`([^`]+)`",t):
            tok=mm.group(1)
            if re.fullmatch(r"[A-Za-z_][\w.]*(\(\))?",tok) and len(tok)>1: out.append((i,"code",tok,l.strip()[:100]))
        t2=re.sub(r"`[^`]*`","",t)
        for pat in PLAIN:
            for mm in re.finditer(pat,t2): out.append((i,"plain",mm.group(0),l.strip()[:100]))
    return sorted(out)
if __name__=="__main__":
    for p in sys.argv[1:]:
        r=scan(p); print(f"#### {p}: {len(r)} bare mentions")
        for i,k,tok,ctx in r: print(f"  L{i} [{k}] {tok} | {ctx}")
