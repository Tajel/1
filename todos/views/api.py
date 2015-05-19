import json
import select

from werkzeug.utils import redirect
from werkzeug.exceptions import NotFound
import psycopg2
from psycopg2 import ProgrammingError
import utc

from todos.framework import View, JSONResponse, Response, reverse
from todos.utils import parse_pgurl


class ApiView(View):
    def __init__(self, *args, **kwargs):
        super(ApiView, self).__init__(*args, **kwargs)
        self.dbconn = psycopg2.connect(**parse_pgurl(self.app.settings.db_url))
        self.dbconn.autocommit = True
        self.db = self.dbconn.cursor()

    def bind_pg_to_websocket(self, filter_id=None):
        self.db.execute('LISTEN todos_updates;')
        while True:
            # Block on notifications from Postgres, with 5 sec timeout.  If we
            # don't hear anything in 5 seconds, send a keepalive ping on the
            # websocket.
            if select.select([self.dbconn], [], [], 5) == ([], [], []):
                self.ws.send_frame('', self.ws.OPCODE_PING)
            else:
                self.dbconn.poll()
                while self.dbconn.notifies:
                    notify = self.dbconn.notifies.pop()
                    payload = json.loads(notify.payload)
                    if filter_id is None or payload['id'] == filter_id:
                        # Handle payloads too big for a PG NOTIFY.
                        if 'error' in payload and 'id' in payload:
                            payload = self.get_todo(payload['id'])
                        self.ws.send(json.dumps(payload))

    def get_todo(self, todo_id):
        self.db.execute("SELECT row_to_json(todos) FROM todos WHERE id=%s", (todo_id,))
        row = self.db.fetchone()
        return row[0] if row else None


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
        for t in self.get_todos():
            self.ws.send(json.dumps(t[0]))

        self.bind_pg_to_websocket()


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
        todo = self.get_todo(todo_id)
        self.ws.send(json.dumps(todo))
        self.bind_pg_to_websocket(filter_id=todo_id)
