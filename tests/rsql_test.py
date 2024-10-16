import sys
sys.path.append("src")
import time, rsql

def hash(x):
    if isinstance(x, dict):
        return hash(frozenset(x.items()))
    return x.__hash__()

N=0 

def test(t, f):
    global N
    N += 1
    rows = t.fetchall()
    t.insert_cbs.append(lambda row: rows.append(tuple([row[k] for k in t.columns])))
    t.delete_cbs.append(lambda row: print("deleting", row, "from", rows) or rows.remove(tuple([row[k] for k in t.columns])))
    t.update_cbs.append(lambda old, new: rows.remove(tuple([old[k] for k in t.columns])) or rows.append(tuple([new[k] for k in t.columns])))
    f()
    t.insert_cbs.pop()
    t.delete_cbs.pop()
    t.update_cbs.pop()
    rows.sort(key=hash)
    rows2 = t.fetchall()
    rows2.sort(key=hash)
    assert rows == rows2, (rows, rows2, t.query)

def assert_eq(x, y, msg=None):
    assert x == y, (x, y, msg)

def assign(l, l2):
    l.clear()
    l.extend(l2)

def test_sort1(t, f):
    global N
    N += 1
    rows = t.fetchall()
    t.insert_cbs.append(lambda index, row: print("insert called", index, row, rows) or rows.insert(index, tuple([row[k] for k in t.columns])))
    t.delete_cbs.append(lambda index, row: assert_eq(rows.pop(index), tuple([row[k] for k in t.columns])))
    t.update_cbs.append(lambda old_index, new_index, old, new:
                        assert_eq(rows.pop(old_index), old, t.query) or rows.insert(new_index, new, t.query))
    t.reset_cbs.append(lambda: assign(rows, t.fetchall()))
    f()
    t.insert_cbs.pop()
    t.delete_cbs.pop()
    t.update_cbs.pop()
    t.reset_cbs.pop()
    rows2 = t.fetchall()
    assert rows == rows2, (rows, rows2, t.query)
    assert rows == t.sorted_results, (rows, t.sorted_results, t.query)

def test_where():
    t = db.table("t", id=int, name=str)
    t.insert(id=1, name="a")
    t.insert(id=2, name="b")
    w = t.where(name="a")
    test(w, lambda: t.insert(id=3,name="a"))
    test(w, lambda: t.insert(id=3, name="b"))
    test(w, lambda: t.delete(id=3, name="a"))
    test(w, lambda: t.update({"name": "b"}, name="c"))
    w.print()
    w.set_filter(name="c")
    w.print()

def test_select():
    t = db.table("t", id=int, name=str)
    t.insert(id=1, name="a")
    t.insert(id=2, name="b")
    w = t.select(name=True)
    test(w, lambda: t.insert(id=3,name="a"))
    test(w, lambda: t.insert(id=3,name=None))
    test(w, lambda: t.insert(id=3, name="b"))
    test(w, lambda: t.delete(id=3, name="a"))
    test(w, lambda: t.update({"name": "b"}, name="c"))

def test_distinct():
    t = db.table("t", id=int, name=str)
    t.insert(id=1, name="a")
    t.insert(id=2, name="b")
    w = t.distinct()
    test(w, lambda: t.insert(id=3,name="a"))
    test(w, lambda: t.insert(id=3, name=None))
    test(w, lambda: t.insert(id=3, name="b"))
    test(w, lambda: t.delete(id=3, name="a"))
    test(w, lambda: t.update({"name": "b"}, name="c"))

def test_union():
    t = db.table("t", id=int, name=str)
    t2 = db.table("t2", id=int, name=str)
    t.insert(id=1, name="a")
    t.insert(id=2, name="b")
    w = t.union(t2)
    test(w, lambda: t.insert(id=3,name="a"))
    test(w, lambda: t.insert(id=3, name=None))
    test(w, lambda: t.insert(id=3, name="b"))
    test(w, lambda: t.delete(id=3, name="a"))
    test(w, lambda: t.update({"name": "b"}, name="c"))


def test_union_all():
    t = db.table("t", id=int, name=str)
    t2 = db.table("t2", id=int, name=str)
    t.insert(id=1, name="a")
    t.insert(id=2, name="b")
    w = t.union_all(t2)
    test(w, lambda: t.insert(id=3,name="a"))
    test(w, lambda: t.insert(id=3, name=None))
    test(w, lambda: t.insert(id=3, name="b"))
    test(w, lambda: t.delete(id=3, name="a"))
    test(w, lambda: t.update({"name": "b"}, name="c"))

def test_join(left_outer=False, right_outer=False):
    t1 = db.table("t6", id=int, name=str)
    t2 = db.table("t7", id=int, value=int)
    
    t1.insert(id=1, name="a")
    t1.insert(id=2, name="b")
    t2.insert(id=1, value=10)
    t2.insert(id=2, value=20)
    
    w = t1.join(t2, id="id", left_outer=left_outer, right_outer=right_outer)
    test(w, lambda: t1.insert(id=2, name="c"))
    test(w, lambda: t2.insert(id=1, value=30))
    test(w, lambda: t1.insert(id=3, name=None))
    test(w, lambda: t1.delete(id=2, name="b"))
    test(w, lambda: t2.update({"value": 25}, id=2))
    test(w, lambda: t1.update({"name": "d"}, id=1))
    test(w, lambda: t1.insert(id=5, name="c"))
    test(w, lambda: t1.update({"id": 7}, id=5))
    test(w, lambda: t1.delete(id=7, name="c"))
    test(w, lambda: t2.insert(id=6, value=40))
    test(w, lambda: t2.update({"value": 45}, id=6))
    test(w, lambda: t1.delete(id=6, name="c"))
    test(w, lambda: t2.delete())
    test(w, lambda: t1.delete())
    test(w, lambda: t1.insert(id=10, name="a"))
    w.print()
    test(w, lambda: t2.insert(id=10, value=100))
    test(w, lambda: t1.update({"id": 10},  name="e"))
    test(w, lambda: t2.update({"id": 10}, value=105))
    w.print()
    test(w, lambda: t1.delete())
    test(w, lambda: t2.delete())
    # w.__del__()


def test_join_no_on(left_outer=False, right_outer=False):
    t1 = db.table("t6", id=int, name=str)
    t2 = db.table("t7", id=int, value=int)
    
    t1.insert(id=1, name="a")
    t1.insert(id=2, name="b")
    t2.insert(id=1, value=10)
    t2.insert(id=2, value=20)
    
    w = t1.join(t2, left_outer=left_outer, right_outer=right_outer)
    test(w, lambda: t1.insert(id=2, name="c"))
    test(w, lambda: t2.insert(id=1, value=30))
    test(w, lambda: t1.insert(id=3, name=None))
    test(w, lambda: t1.delete(id=2, name="b"))
    test(w, lambda: t2.update({"value": 25}, id=2))
    test(w, lambda: t1.update({"name": "d"}, id=1))
    test(w, lambda: t1.insert(id=5, name="c"))
    test(w, lambda: t1.update({"id": 7}, id=5))
    test(w, lambda: t1.delete(id=7, name="c"))
    test(w, lambda: t2.insert(id=6, value=40))
    test(w, lambda: t2.update({"value": 45}, id=6))
    test(w, lambda: t1.delete(id=6, name="c"))
    test(w, lambda: t2.delete())
    test(w, lambda: t1.delete())
    test(w, lambda: t1.insert(id=10, name="a"))
    w.print()
    test(w, lambda: t2.insert(id=10, value=100))
    test(w, lambda: t1.update({"id": 10},  name="e"))
    test(w, lambda: t2.update({"id": 10}, value=105))
    w.print()
    test(w, lambda: t1.delete())
    test(w, lambda: t2.delete())
    # w.__del__()

def test_group_by():
    t = db.table("t8", id=int, name=str, value=int)
    t.insert(id=1, name="a", value=10)
    t.insert(id=2, name="b", value=20)
    t.insert(id=3, name="a", value=30)
    t.insert(id=4, name="b", value=40)
    
    w = t.group_by("name", sum_value="SUM(value)", min_value="MIN(value)", max_value="MAX(value)", avg_value="AVG(value)")
    
    test(w, lambda: t.insert(id=5, name="a", value=50))
    test(w, lambda: t.insert(id=6, name="c", value=60))
    test(w, lambda: t.delete(id=3, name="a", value=30))
    test(w, lambda: t.update({"value": 25}, id=2))
    test(w, lambda: t.update({"name": "c"}, id=1))
    test(w, lambda: t.delete(id=6, name="c", value=60))

def test_group_by_all():
    t = db.table("t9", id=int, name=str, value=int)
    t.insert(id=1, name="a", value=10)
    t.insert(id=2, name="b", value=20)
    t.insert(id=3, name="a", value=30)
    t.insert(id=4, name="b", value=40)
    t.insert(id=5, name=None, value=50)
    
    w = t.group_by(sum_value="SUM(value)", min_value="MIN(value)", max_value="MAX(value)", avg_value="AVG(value)")
    
    test(w, lambda: t.insert(id=5, name="a", value=50))
    test(w, lambda: t.insert(id=6, name="c", value=60))
    test(w, lambda: t.delete(id=3, name="a", value=30))
    test(w, lambda: t.update({"value": 25}, id=2))
    test(w, lambda: t.update({"name": "c"}, id=1))
    test(w, lambda: t.delete(id=6, name="c", value=60))
    test(w, lambda: t.delete())
    test(w, lambda: t.insert(id=5, name="a", value=50))

def test_sort():
    t = db.table("t10", id=int, name=str)
    t.insert(id=1, name="a")
    t.insert(id=2, name="b")
    t.insert(id=3, name="a")
    t.insert(id=4, name="b")
    w = t.sort(order_by="name")
    test_sort1(w, lambda: t.insert(id=5, name="a"))
    test_sort1(w, lambda: t.insert(id=6, name="c"))
    test_sort1(w, lambda: t.insert(id=7, name=None))
    test_sort1(w, lambda: t.delete(id=3, name="a"))
    test_sort1(w, lambda: t.update({"name": "d"}, id=1))
    test_sort1(w, lambda: t.delete(id=6, name="c"))

def test_sort_limit():
    t = db.table("t11", id=int, name=str)
    t.insert(id=1, name="a")
    t.insert(id=2, name="b")
    t.insert(id=3, name=None)
    t.insert(id=3, name="a")
    t.insert(id=4, name="b")
    w = t.sort(order_by="name", limit=2)
    test_sort1(w, lambda: w.set_limit(3))
    test_sort1(w, lambda: w.set_limit(2))
    test_sort1(w, lambda: w.set_offset(2))
    test_sort1(w, lambda: w.set_offset(0))
    test_sort1(w, lambda: t.insert(id=5, name="a"))
    test_sort1(w, lambda: t.insert(id=6, name="c"))
    test_sort1(w, lambda: t.delete(id=3, name="a"))
    test_sort1(w, lambda: t.update({"name": "d"}, id=1))
    test_sort1(w, lambda: t.delete(id=6, name="c"))

changedvalue = None
currentvalue = None

def value_test():
    db = rsql.Database(":memory:")
    nextstep = db.table("nextstep", nextstep=int)
    nextstep.delete()
    nextstep.insert(nextstep=1)
    o=nextstep.only()
    def setchangedvalue(x):
        global changedvalue
        changedvalue = x
    def setcurrentvalue(x):
        global currentvalue
        currentvalue = x
    o.onchange(lambda x: setchangedvalue(x))
    o.onvalue(lambda x: setcurrentvalue(x))
    assert_eq(changedvalue, None)
    assert_eq(currentvalue['nextstep'], 1)
    nextstep.update({}, nextstep=2)
    assert_eq(changedvalue['nextstep'], 2)
    assert_eq(currentvalue['nextstep'], 2)

t=time.time()
db = rsql.Database(":memory:")
t0 = time.time()
N0 = N
test_select()
print(f"select: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_where()
print(f"where: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_distinct()
print(f"distinct: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_union()
print(f"union: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_union_all()
print(f"union_all: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
for left_outer in [False, True]:
    for right_outer in [False, True]:
        test_join(left_outer, right_outer)
        test_join_no_on(left_outer, right_outer)
print(f"join: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_group_by()
print(f"group_by: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_group_by_all()
print(f"group_by_all: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_sort()
print(f"sort: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
t0 = time.time()
N0 = N
test_sort_limit()
print(f"sort_limit: {N-N0} ops in {time.time()-t0:.2f}s, {(time.time()-t0)/(N-N0)*1000000:.2f} μs/op")
print(f"All table tests pass ({N} ops in {time.time()-t:.2f}s),  {((time.time()-t)/N)*1000000:.2f} μs/op")
value_test()


def map_value_test():
    v = rsql.Value(1)
    m = rsql.MapValue(v, lambda x: x*2)
    assert_eq(m.value, 2)
    



def html_test():
    db=rsql.Database(":memory:")
    a=db.table("a", b=int) 
    a.insert(b=2)
    o=a.only() # RowValue
    print(o.value)
html_test()