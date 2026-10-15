"""Lists UNLINKED mentions under the 'every mention of a method or feature is a link' rule.
Skips fenced code, headings, front matter, link text and destinations (inline, multiline,
reference-style, and shortcut reference links), and URLs. Prints every bare occurrence (not just
the first per paragraph). The reader decides which are columns, user code, JDK names, parameters,
or literal values (those are not linked).
Fences are found first (backtick and tilde, tracked by delimiter and length, inside indentation and
blockquotes) and blanked, then links are masked in the remaining prose, so a stray bracket in code
cannot swallow a fence or later prose. Line numbers are preserved. Run with `python3 -I`."""
import re,sys
PLAIN=[r"\bBarriers?\b",r"\bformula strings?\b",r"\bserial execution\b",r"\bpredicate pushdown\b",r"\bpartitioned tables?\b",r"\bConcurrencyControl\b",r"\bSelectables?\b",r"\bFilters?\b(?= objects?\b| instances?\b)"]
FENCE=re.compile(r"^[ \t>]*(`{3,}|~{3,})(.*)$")
def prose_lines(lines):
    """Return the lines with fenced code and front matter blanked."""
    out=[]; fence=None; fm=False
    for i,l in enumerate(lines):
        if i==0 and l.strip()=="---": fm=True; out.append(""); continue
        if fm:
            if l.strip()=="---": fm=False
            out.append(""); continue
        m=FENCE.match(l)
        if fence is None:
            if m and not (m.group(1)[0]=="`" and "`" in m.group(2)):
                fence=(m.group(1)[0],len(m.group(1))); out.append(""); continue
            out.append(l)
        else:
            if m and m.group(1)[0]==fence[0] and len(m.group(1))>=fence[1] and not m.group(2).strip(): fence=None
            out.append("")
    return out
def norm(label): return re.sub(r"\s+"," ",label.strip()).lower()
LABEL=r"\[(?:[^\[\]\n]|\n(?!\s*\n))*\]"     # a link label: no nested brackets, no blank line
def protect_code(text):
    """Hide brackets and parentheses inside inline code spans so they cannot start or end a link."""
    tr=str.maketrans("[]()","\x01\x02\x03\x04")
    return re.sub(r"`[^`\n]*`",lambda m: m.group(0).translate(tr),text)
def restore_code(text): return text.translate(str.maketrans("\x01\x02\x03\x04","[]()"))
def mask_links(text):
    """Blank links, keeping line numbers."""
    keep=lambda m: "\n"*m.group(0).count("\n")
    text=protect_code(text)
    labels={norm(m.group(1)) for m in re.finditer(r"(?m)^[ \t>]*\[((?:[^\]\n])+)\]:[ \t]*\S",text)}
    text=re.sub(r"(?m)^[ \t>]*\[[^\]\n]+\]:[ \t]*\S.*$",keep,text)                  # reference definitions
    text=re.sub(LABEL+r"\((?:[^()\n]|\n(?!\s*\n))*\)",keep,text)                        # inline links
    text=re.sub(LABEL+r"\[[^\[\]\n]*\]",keep,text)                                      # [text][ref] and [text][]
    def shortcut(m): return keep(m) if norm(m.group(1)[1:-1]) in labels else m.group(0)
    text=re.sub("("+LABEL+r")(?![\[(:])",shortcut,text)                                    # shortcut [label]
    return restore_code(text)
def scan(path):
    out=[]; raw=open(path,encoding="utf-8").read().split("\n")
    lines=mask_links("\n".join(prose_lines(raw))).split("\n")
    for i,l in enumerate(lines,1):
        if l.lstrip(" \t>").startswith("#"): continue
        t=re.sub(r"https?://[^\s`)>\]]+"," ",l)       # drop bare URLs, keep backticks paired
        for mm in re.finditer(r"`([^`]+)`",t):
            tok=mm.group(1)
            if re.fullmatch(r"[A-Za-z_][\w.]*(\(\))?",tok) and len(tok)>1: out.append((i,"code",tok,raw[i-1].strip()[:100]))
        t2=re.sub(r"`[^`]*`","",t)
        for pat in PLAIN:
            for mm in re.finditer(pat,t2): out.append((i,"plain",mm.group(0),raw[i-1].strip()[:100]))
    return sorted(out)
if __name__=="__main__":
    for p in sys.argv[1:]:
        r=scan(p); print(f"#### {p}: {len(r)} bare mentions")
        for i,k,tok,ctx in r: print(f"  L{i} [{k}] {tok} | {ctx}")
