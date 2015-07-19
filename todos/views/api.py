import json
from Queue import Empty

import pgpubsub
import psycopg2
from werkzeug.utils import redirect
from werkzeug.exceptions import NotFound

from todos.framework import View, JSONResponse, Response, reverse
from todos.utils import parse_pgurl


class ApiView(View):
    def __init__(self, *args, **kwargs):
        super(ApiView, self).__init__(*args, **kwargs)
        self.dbconn = psycopg2.connect(**parse_pgurl(self.app.settings.db_url))
        self.dbconn.autocommit = True
        self.db = self.dbconn.cursor()
        self.pubsub = pgpubsub.PubSub(self.dbconn)

    def get_todo(self, todo_id):
        self.db.execute("SELECT row_to_json(todos) FROM todos WHERE id=%s", (todo_id,))
        row = self.db.fetchone()
        return row[0] if row else None

    def send_pubsub_events(self, filter_events):
        msg_q = self.app.fanout.subscribe(filter_events)
        try:
            while True:
                try:
                    event = msg_q.get(True, timeout=5)
                    if event is None:
                        # The fanout will send a None in the queue if it has
                        # been told to exit.
                        break
                    self.ws.send(event.payload)
                except Empty:
                    self.ws.send_frame('', self.ws.OPCODE_PING)
        finally:
            self.app.fanout.unsubscribe(msg_q)


class TodoList(ApiView):

    def get_todos(self):
        self.db.execute("SELECT row_to_json(todos) FROM todos ORDER BY created_time;")
        return self.db

    def get(self):
        return JSONResponse({
            'objects': [t[0] for t in self.get_todos()]
        })

    def post(self):
        title = json.loads(self.request.data)['title']
        self.db.execute("INSERT INTO todos (title) VALUES (%s) RETURNING id", (title,))
        uuid = self.db.fetchone()[0]
        url = reverse(self.app.map, 'todo_detail', {'todo_id': uuid})
        return redirect(url)

    def websocket(self):
        # First send out all the existing ones.
        for t in self.get_todos():
            self.ws.send(json.dumps(t[0]))
        self.send_pubsub_events(lambda x: True)



class TodoDetail(ApiView):

    def get(self, todo_id):
        todo = self.get_todo(todo_id)
        if todo is None:
            return NotFound()
        return JSONResponse(todo)

    def put(self, todo_id):
        todo = json.loads(self.request.data)
        query = "UPDATE todos SET title=%s, completed=%s WHERE id=%s RETURNING id;"
        self.db.execute(query, (todo['title'], todo['completed'], todo_id))
        updated = self.db.fetchone()
        if updated is None:
            return NotFound()
        url = reverse(self.app.map, 'todo_detail', {'todo_id': todo_id})
        return redirect(url)

    def delete(self, todo_id):
        self.db.execute("DELETE FROM todos WHERE id=%s RETURNING id;",
                        (todo_id,))
        deleted = self.db.fetchone()
        if deleted is None:
            return NotFound()
        return Response()

    def websocket(self, todo_id):
        # first send out the data for this todo.
        todo = self.get_todo(todo_id)
        self.ws.send(json.dumps(todo))

        # Subscribe to pubsub, with filter function that only returns events
        # about this todo_id.
        def filter_events(event):
            if event.channel != 'todos_updates':
                return False
            parsed = json.loads(event.payload)
            return parsed.get('id') == todo_id
        self.send_pubsub_events(filter_events)
