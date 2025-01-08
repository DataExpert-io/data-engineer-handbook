from flask import Flask, jsonify, request
from statsig import statsig
from statsig.statsig_event import StatsigEvent
from statsig.statsig_user import StatsigUser
import random
import os

API_KEY = os.environ.get('STATSIG_API_KEY')
statsig.initialize(API_KEY)
app = Flask(__name__)

# Sample in-memory database
tasks = [
    {
        'id': 1,
        'title': 'Do the dishes',
        'description': 'Odd Tasks',
        'done': False
    },
    {
        'id': 2,
        'title': 'Study for exam',
        'description': 'Even Tasks',
        'done': False
    }
]


@app.route('/')
def hello():
    return "Hello, this is a Flask API!"


@app.route('/signup')
def signup():
    random_num = request.args.get('random')
    hash_string = request.remote_addr
    if random_num:
        hash_string = str(random.randint(0, 1000000))
    user_id = str(hash(hash_string))
    statsig_user = StatsigUser(user_id)
    statsig_event = StatsigEvent(
        user=statsig_user,
        event_name='visited_signup'
    )
    statsig.log_event(statsig_event)
    return "This is the signup page"


@app.route('/tasks', methods=['GET'])
def get_tasks():
    random_num = request.args.get('random')
    hash_string = request.remote_addr
    if random_num:
        hash_string = str(random.randint(0, 1000000))
    user_id = str(hash(hash_string))
    color = statsig.get_experiment(StatsigUser(user_id), "button_color_v3").get("Button Color", "blue")
    paragraph_text = statsig.get_experiment(StatsigUser(user_id), "button_color_v3").get("Paragraph Text", "Data Engineering Boot Camp")
    experiment_description = 'odd tasks for blue and green, even for red and orange'
    filtered_tasks = ''.join(map(lambda a: f"""
            <tr>
            <td>
                {a['id']}
            </td>
            <td>
                {a['title']}
            </td>
            <td>
                {a['description']}
            </td> 
            <td>
                {a['done']}
            </td>

            </tr>
        """, list(filter(lambda x: x['id'] % 2 == (0 if color == 'Red' or color == 'Orange' else 1), tasks))))
    return f"""
        <div style="{"background: " + color}">
        <h1>{"experiment group: " + color}</h1>
        <h2>{"experiment description: " + experiment_description}</h2>
        <h5>{"current user identifier is:   <i>" + user_id}</i></h5>
        <h5>{paragraph_text}</h5>
        <table>
            <thead>
                <th>Id</th>
                <th>Title</th>
                <th>Description</th>
                <th>Done</th>
            </thead>
            <tbody>
            {filtered_tasks}
            </tbody>
        </table>
        <a href="/signup">Go to Signup</a>
        </div>
    """

@app.route('/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = next((task for task in tasks if task['id'] == task_id), None)
    if task:
        return jsonify({'task': task})
    return jsonify({'error': 'Task not found'}), 404


@app.route('/tasks', methods=['POST'])
def create_task():
    if not request.json or not 'title' in request.json:
        return jsonify({'error': 'The new task must have a title'}), 400
    task = {
        'id': tasks[-1]['id'] + 1 if tasks else 1,
        'title': request.json['title'],
        'description': request.json.get('description', ""),
        'done': False
    }
    tasks.append(task)
    return jsonify({'task': task}), 201


@app.route('/tasks/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    task = next((task for task in tasks if task['id'] == task_id), None)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    if not request.json:
        return jsonify({'error': 'Malformed request'}), 400
    task['title'] = request.json.get('title', task['title'])
    task['description'] = request.json.get('description', task['description'])
    task['done'] = request.json.get('done', task['done'])
    return jsonify({'task': task})


@app.route('/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    global tasks
    tasks = [task for task in tasks if task['id'] != task_id]
    return jsonify({'result': True})


if __name__ == '__main__':
    app.run(debug=True)
