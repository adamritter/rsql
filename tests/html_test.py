# Warning: this must be rewritten as HTML queries instead of testing internals.
import sys
sys.path.append("src")
from rsql.html import *
def assert_eq(a, b):
    assert a == b, f"a {a} != b {b}"

def with_variable():
    db = rsql.Database(":memory:")
    t = db.table("a", b=int)
    t.insert(b=1)
    app, rtx = rsql_html_app(db=db)
    v=t.only()
    assert_eq(v.__html__(), """<span id="e1">&lt;Row a {'id': 1, 'b': 1}></span>""")
    print(objects_per_tab)
    print("queues", queues)
    t.update({}, b=2)
    print("queues after update", queues)
    h2=queues[tab_id.get()].get()
    assert_eq(h2.__html__(), """<span id="e1" hx-swap-oob="true">&lt;Row a {'id': 1, 'b': 2}></span>""")

# Now without serialization, harder
def without_variable():
    db = rsql.Database(":memory:")
    t = db.table("a", b=int)
    t.insert(b=1)
    # reset
    rsql.html.lastid = 0
    objects_per_tab.clear()
    queues.clear()
    app, rtx = rsql_html_app(db=db)
    print("without variable")
    assert_eq(t.only().__html__(), """<span id="e1">&lt;Row a {'id': 1, 'b': 1}></span>""")
    print(objects_per_tab)
    print("queues", queues)
    t.update({}, b=2)
    print("queues after update", queues)
    h2=queues[tab_id.get()].get()
    assert_eq(h2.__html__(), """<span id="e1" hx-swap-oob="true">&lt;Row a {'id': 1, 'b': 2}></span>""")

with_variable()
without_variable()

