"""Regression tests for scan_unlinked_mentions.py. Run: python3 -I test_scan_unlinked_mentions.py"""
import os,sys,tempfile,unittest
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
from scan_unlinked_mentions import scan
def found(text):
    with tempfile.TemporaryDirectory() as d:
        p=os.path.join(d,"page.md"); open(p,"w",encoding="utf-8").write(text)
        return [(i,tok) for i,_,tok,_ in scan(p)]
class Scan(unittest.TestCase):
    def test_prose_is_reported(self): self.assertEqual(found("Bare `where` here.\n"),[(1,"where")])
    def test_fences_are_skipped(self):
        t="    ```py\n    `where`\n    ```\n\n> ```\n> `where`\n> ```\n\n~~~\n`where`\n~~~\n\n````md\n```\n`where`\n```\n````\n\nAfter `select`.\n"
        self.assertEqual(found(t),[(19,"select")])
    def test_stray_bracket_in_fence_does_not_swallow_prose(self):
        t='```python\nmarker = "["\n```\n\nLink [`update`](./u.md) and bare `select`.\n'
        self.assertEqual(found(t),[(5,"select")])
    def test_inline_multiline_and_reference_links(self):
        t="See [`a_b`][ref] and [`c_d`](\n./x.md) and bare `select`.\n\n[ref]: ./a.md\n"
        self.assertEqual(found(t),[(2,"select")])
    def test_shortcut_reference_link(self):
        self.assertEqual(found("Use [`where`] and bare `select`.\n\n[`where`]: where.md\n"),[(1,"select")])
    def test_unrelated_brackets_do_not_hide_mentions(self):
        self.assertEqual(found("Use [`select`] and [`where`](where.md).\n"),[(1,"select")])
        self.assertEqual(found("Use [`select`] and [`where`][w].\n\n[w]: where.md\n"),[(1,"select")])
        self.assertEqual(found("Use `arr[0]` and `select` with [`where`](where.md).\n"),[(1,"select")])
        self.assertEqual(found("Use `f(x)` and `select` with [`where`](where.md).\n"),[(1,"select")])
    def test_url_in_backticks_keeps_later_mentions(self):
        self.assertEqual(found("Use `https://example.org` with `where`.\n"),[(1,"where")])
    def test_headings_and_front_matter_are_skipped(self):
        self.assertEqual(found("---\ntitle: `where`\n---\n\n## The `where` method\n"),[])
if __name__=="__main__": unittest.main()
