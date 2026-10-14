"""Lists UNLINKED mentions under the 'every mention of a method or feature is a link' rule.
Skips fenced code, headings, front matter, link text/targets, URLs. Prints every bare occurrence
(not just the first per paragraph). The reader decides which are columns, user code, JDK names,
parameters, or literal values (those are not linked)."""
import re,sys
PLAIN=[r"\bBarriers?\b",r"\bformula strings?\b",r"\bserial execution\b",r"\bpredicate pushdown\b",r"\bpartitioned tables?\b",r"\bConcurrencyControl\b",r"\bSelectables?\b",r"\bFilters?\b(?= objects?\b| instances?\b)"]
def scan(path):
    out=[]; lines=open(path).read().split("\n"); infence=False; fm=False
    for i,l in enumerate(lines,1):
        if i==1 and l.strip()=="---": fm=True; continue
        if fm:
            if l.strip()=="---": fm=False
            continue
        if l.startswith("```"): infence=not infence; continue
        if infence or l.lstrip().startswith("#"): continue
        t=re.sub(r"\[[^\]]*\]\([^)]*\)","",l)          # drop existing links (text and target)
        t=re.sub(r"https?://\S+","",t)
        for m in re.finditer(r"`([^`]+)`",t):
            tok=m.group(1)
            if re.fullmatch(r"[A-Za-z_][\w.]*(\(\))?",tok) and len(tok)>1: out.append((i,"code",tok,l.strip()[:100]))
        t2=re.sub(r"`[^`]*`","",t)
        for pat in PLAIN:
            for m in re.finditer(pat,t2): out.append((i,"plain",m.group(0),l.strip()[:100]))
    return sorted(out)
if __name__=="__main__":
    for p in sys.argv[1:]:
        r=scan(p); print(f"#### {p}: {len(r)} bare mentions")
        for i,k,tok,ctx in r: print(f"  L{i} [{k}] {tok} | {ctx}")
