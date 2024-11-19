from rsql.html import *
db = Database(":memory:")
t = db.table("t", txt=str)
app, rtx = rsql_html_app(db=db)

def ajax_str(state:int, timeout:int=0):
    return f"setTimeout(function() {{ htmx.ajax('POST', '/check', {{values: {{state: {state}, inner_html: document.getElementById('t').innerHTML}}}}) }}, {timeout})"

@rtx('/')
def get():
    return Div(H1("Table"), table(t, id="t"),
              Script(ajax_str(0, 100)))

from html.parser import HTMLParser
from selenium import webdriver
DEBUG_HTML = os.environ.get('DEBUG_HTML', 'False').lower() in ['true', '1', 'yes', 'on']
class TestHTMLParser(HTMLParser):
    def __init__(self):
        self.rows = []
        self.row = []
        self.intd = False
        super().__init__()
    def handle_starttag(self, tag, attrs):
        if DEBUG_HTML:
            print("starttag", tag, attrs)
        if tag == "tr":
            self.row = []
        if tag == "td":
            self.intd = True
        if tag == "tbody":
            raise Exception(f"tbody starttag: {attrs}")

    def handle_endtag(self, tag):
        if DEBUG_HTML:
            print("endtag", tag)
        if tag == "tr":
            self.rows.append(tuple(self.row))
            self.row = []
        if tag == "td":
            self.intd = False

    def handle_data(self, data):
        if DEBUG_HTML:
            print("data", data)
        if self.intd:
            self.row.append(data)
import sys, signal, os

@rtx('/check')
def post(inner_html:str, state:int):
    parser = TestHTMLParser()
    parser.feed(inner_html)
    s1 = sorted(parser.rows)    
    s2 = sorted([tuple(map(str, tu)) for tu in t.fetchall()])
    if str(s1) != str(s2):
        print("not equal: ", s1, s2)
        return Script(f"console.log('not equal, state={state}')")
    if state == 0:
        t.insert(txt="hello")
        return Script(ajax_str(1))
    elif state == 1:
        t.insert(txt="hello2")
        return Script(ajax_str(2))
    elif state == 2:
        t.delete(txt="hello")
        return Script(ajax_str(3))
    elif state == 3:
        t.update({"txt":"hello2"}, txt="hello3")
        return Script(ajax_str(4))
    else:
        print(f"**** OK, state={state} ****")
        os.kill(os.getpid(), signal.SIGTERM)
        return Script(f"console.log('ok, state={state}')")
    
def selenium_test_thread():
    import time
    from selenium.webdriver.firefox.options import Options
    time.sleep(0.1)
    options = Options()
    options.add_argument('-headless')
    driver = webdriver.Firefox(options=options)
    driver.get("http://localhost:5010/")
    time.sleep(10)
    driver.quit()

if __name__ == "__main__":
    threading.Thread(target=selenium_test_thread).start()
    serve(port=5010, reload=False)
