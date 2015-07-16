# Python + Postgres + React TodoMVC Example

This application takes the React TodoMVC example and adds a Python and Postgres
backend.

All changes are immediately stored in Postgres (instead of browser localStorage
as in the upstream example), and then streamed out to clients via websockets.

To get it running:

1. Install [Vagrant](https://www.vagrantup.com/downloads.html).
2. Clone the repository at https://bitbucket.org/btubbs/todopy-pg.
3. From the root of the repository, type `vagrant up`.  Wait for the
provisioning script to finish.
4. Type `vagrant ssh` to get a shell into the Vagrant environment.
5. `cd /vagrant`
6. `mkvirtualenv todos`
7. `make dev`
8. `foreman start`

You can now browse to the application at http://localhost:8000/.  Open it in
multiple tabs, and you should see your changes to Todo items applied at the same
time on all of them.

You can watch the presentation that accompanies this code at
https://youtu.be/PsorlkAF83s.
