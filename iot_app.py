from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import paho.mqtt.client as mqtt
import sqlite3
from datetime import datetime
import logging
from werkzeug.security import generate_password_hash, check_password_hash
import random

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Set a fixed secret key for session persistence
app.secret_key = '3427c80e7f70ff707e1031e191d47470539ad370d9481692'

# MQTT Configuration
mqttBroker = "iot-dashboard.cloud.shiftr.io"
mqttUser = "iot-dashboard"
mqttPassword = "YBxsZiVmkHljoCId"
mqttClient = mqtt.Client(client_id=f"iot-dashboard-{random.randint(0, 10000)}", protocol=mqtt.MQTTv311)

mqttClient.username_pw_set(mqttUser, mqttPassword)

# Initialize database
def init_db():
    conn = sqlite3.connect('iot_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            email TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            name TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS temperature_humidity_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            timestamp TEXT,
            humidity REAL,
            temperature REAL,
            FOREIGN KEY(device_id) REFERENCES devices(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS light_sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            timestamp TEXT,
            light_level REAL,
            FOREIGN KEY(device_id) REFERENCES devices(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS relay_lights_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            timestamp TEXT,
            status INTEGER,
            FOREIGN KEY(device_id) REFERENCES devices(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS relay_fans_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            timestamp TEXT,
            status INTEGER,
            FOREIGN KEY(device_id) REFERENCES devices(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dc_motor_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            timestamp TEXT,
            direction TEXT,
            FOREIGN KEY(device_id) REFERENCES devices(id)
        )
    ''')
    conn.commit()
    conn.close()

# MQTT Sensor Data Handler
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logging.info("Connected to MQTT Broker!")
        client.subscribe("home/sensors/temperature", qos=1)
        client.subscribe("home/sensors/humidity", qos=1)
        client.subscribe("home/sensors/light", qos=1)
        client.subscribe("home/control/light", qos=1)
        client.subscribe("home/control/fan", qos=1)
        client.subscribe("home/control/motor", qos=1)
    else:
        logging.error(f"Failed to connect, return code {reason_code}")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    try:
        if topic == "home/sensors/temperature":
            store_sensor_data(temperature=float(payload))
        elif topic == "home/sensors/humidity":
            store_sensor_data(humidity=float(payload))
        elif topic == "home/sensors/light":
            store_sensor_data(light=float(payload))
        elif topic == "home/control/light":
            store_light_status(payload, 1)
        elif topic == "home/control/fan":
            store_fan_status(payload)
        elif topic == "home/control/motor":
            store_motor_status(payload.lower())
        app.logger.debug(f"[✓] Processed: {payload} from {topic}")
    except Exception as e:
        app.logger.error(f"[✗] Failed to process message from {topic}: {e}")

# Database Functions
def store_sensor_data(temperature=None, humidity=None, light=None):
    try:
        conn = sqlite3.connect("iot_data.db")
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()
        if temperature is not None and humidity is not None:
            cursor.execute(
                "INSERT INTO temperature_humidity_data (device_id, timestamp, humidity, temperature) VALUES (?, ?, ?, ?)",
                (1, timestamp, humidity, temperature)
            )
        if light is not None:
            cursor.execute(
                "INSERT INTO light_sensor_data (device_id, timestamp, light_level) VALUES (?, ?, ?)",
                (1, timestamp, light)
            )
        conn.commit()
    except Exception as e:
        app.logger.error(f"Error in store_sensor_data: {e}")
    finally:
        conn.close()

def store_light_status(action, device_id):
    try:
        conn = sqlite3.connect('iot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, timestamp FROM relay_lights_status WHERE device_id = ? ORDER BY id DESC LIMIT 1",
            (device_id,)
        )
        last_entry = cursor.fetchone()
        if last_entry and (datetime.now() - datetime.fromisoformat(last_entry[1])).total_seconds() < 1:
            if last_entry[0] == (1 if action == 'ON' else 0):
                app.logger.debug(f"Skipping duplicate light status: {action} for device {device_id}")
                return
        cursor.execute(
            "INSERT INTO relay_lights_status (device_id, timestamp, status) VALUES (?, ?, ?)",
            (device_id, datetime.now().isoformat(), 1 if action == 'ON' else 0)
        )
        conn.commit()
    except Exception as e:
        app.logger.error(f"Error in store_light_status: {e}")
    finally:
        conn.close()

def store_fan_status(action):
    try:
        conn = sqlite3.connect('iot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, timestamp FROM relay_fans_status WHERE device_id = ? ORDER BY id DESC LIMIT 1",
            (6,)
        )
        last_entry = cursor.fetchone()
        if last_entry and (datetime.now() - datetime.fromisoformat(last_entry[1])).total_seconds() < 1:
            if last_entry[0] == (1 if action == 'ON' else 0):
                app.logger.debug(f"Skipping duplicate fan status: {action}")
                return
        cursor.execute(
            "INSERT INTO relay_fans_status (device_id, timestamp, status) VALUES (?, ?, ?)",
            (6, datetime.now().isoformat(), 1 if action == 'ON' else 0)
        )
        conn.commit()
    except Exception as e:
        app.logger.error(f"Error in store_fan_status: {e}")
    finally:
        conn.close()

def store_motor_status(direction):
    try:
        conn = sqlite3.connect('iot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT direction, timestamp FROM dc_motor_status WHERE device_id = ? ORDER BY id DESC LIMIT 1",
            (3,)
        )
        last_entry = cursor.fetchone()
        if last_entry and (datetime.now() - datetime.fromisoformat(last_entry[1])).total_seconds() < 1:
            if last_entry[0] == direction.lower():
                app.logger.debug(f"Skipping duplicate motor status: {direction}")
                return
        cursor.execute(
            "INSERT INTO dc_motor_status (device_id, timestamp, direction) VALUES (?, ?, ?)",
            (3, datetime.now().isoformat(), direction)
        )
        conn.commit()
    except Exception as e:
        app.logger.error(f"Error in store_motor_status: {e}")
    finally:
        conn.close()

# Flask Routes
@app.route('/')
def index():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        conn = sqlite3.connect('iot_data.db')
        cursor = conn.cursor()
        cursor.execute("SELECT id, password FROM users WHERE username = ?", (username,))
        user = cursor.fetchone()
        conn.close()
        if user and check_password_hash(user[1], password):
            session['user_id'] = user[0]
            return jsonify({'status': 'success'})
        return jsonify({'status': 'error', 'message': 'Invalid credentials'}), 401
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        email = request.form.get('email')
        try:
            conn = sqlite3.connect('iot_data.db')
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (username, password, email) VALUES (?, ?, ?)",
                (username, generate_password_hash(password), email)
            )
            conn.commit()
            conn.close()
            return redirect(url_for('login'))
        except Exception as e:
            app.logger.error(f"Error in register: {e}")
            return jsonify({'status': 'error', 'message': str(e)}), 500
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    return redirect(url_for('login'))

@app.route('/control_led', methods=['POST'])
def control_led():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    action = request.json.get('action')
    device_id = request.json.get('device_id')
    if action not in ['ON', 'OFF'] or not device_id:
        return jsonify({'status': 'error', 'message': 'Invalid action or device_id'}), 400
    try:
        mqttClient.publish("home/control/light", action, qos=1)
        return jsonify({'status': 'success'})
    except Exception as e:
        app.logger.error(f"Error in control_led route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/control_fan', methods=['POST'])
def control_fan():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    action = request.json.get('action')
    if action not in ['ON', 'OFF']:
        return jsonify({'status': 'error', 'message': 'Invalid fan action'}), 400
    try:
        mqttClient.publish("home/control/fan", action, qos=1)
        return jsonify({'status': 'success'})
    except Exception as e:
        app.logger.error(f"Error in control_fan route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/control_motor', methods=['POST'])
def control_motor():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    action = request.json.get('action')
    if action not in ['FORWARD', 'BACKWARD', 'STOP']:
        return jsonify({'status': 'error', 'message': 'Invalid motor action'}), 400
    try:
        mqttClient.publish("home/control/motor", action, qos=1)
        return jsonify({'status': 'success'})
    except Exception as e:
        app.logger.error(f"Error in control_motor route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/sensor_data')
def get_sensor_data():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    try:
        conn = sqlite3.connect('iot_data.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM temperature_humidity_data ORDER BY id DESC LIMIT 1")
        temp_hum = cursor.fetchone()
        cursor.execute("SELECT * FROM light_sensor_data ORDER BY id DESC LIMIT 1")
        light = cursor.fetchone()
        conn.close()
        return jsonify({
            'humidity': temp_hum[3] if temp_hum else None,
            'temperature': temp_hum[4] if temp_hum else None,
            'light': light[3] if light else None
        })
    except Exception as e:
        app.logger.error(f"Error in sensor_data route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/history')
def get_history():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    try:
        conn = sqlite3.connect('iot_data.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, timestamp, humidity, temperature FROM temperature_humidity_data ORDER BY timestamp DESC")
        temp_hum_data = cursor.fetchall()
        cursor.execute("SELECT id, timestamp, light_level FROM light_sensor_data ORDER BY timestamp DESC")
        light_data = cursor.fetchall()
        
        sensors = []
        for th, l in zip(temp_hum_data, light_data):
            sensors.append({
                'id': th[0],
                'timestamp': th[1],
                'humidity': th[2],
                'temperature': th[3],
                'light_level': l[2]
            })
        
        cursor.execute("SELECT id, timestamp, status FROM relay_lights_status WHERE device_id = 1 ORDER BY timestamp DESC")
        led1 = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, status FROM relay_fans_status WHERE device_id = 6 ORDER BY timestamp DESC")
        fan = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, direction FROM dc_motor_status WHERE device_id = 3 ORDER BY timestamp DESC")
        motor = [{'id': row[0], 'timestamp': row[1], 'direction': row[2]} for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'sensors': sensors,
            'led1': led1,
            'fan': fan,
            'motor': motor
        })
    except Exception as e:
        app.logger.error(f"Error in history route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/all_data_visualization')
def get_all_data_visualization():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    try:
        conn = sqlite3.connect('iot_data.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, timestamp, humidity, temperature FROM temperature_humidity_data ORDER BY timestamp DESC")
        temp_hum_data = cursor.fetchall()
        cursor.execute("SELECT id, timestamp, light_level FROM light_sensor_data ORDER BY timestamp DESC")
        light_data = cursor.fetchall()
        
        sensors = []
        for th, l in zip(temp_hum_data, light_data):
            sensors.append({
                'id': th[0],
                'timestamp': th[1],
                'humidity': th[2],
                'temperature': th[3],
                'light_level': l[2]
            })
        
        cursor.execute("SELECT id, timestamp, status FROM relay_lights_status WHERE device_id = 1 ORDER BY timestamp DESC")
        led1 = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, status FROM relay_fans_status WHERE device_id = 6 ORDER BY timestamp DESC")
        fan = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, direction FROM dc_motor_status WHERE device_id = 3 ORDER BY timestamp DESC")
        motor = [{'id': row[0], 'timestamp': row[1], 'direction': row[2]} for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'sensors': sensors,
            'led1': led1,
            'fan': fan,
            'motor': motor
        })
    except Exception as e:
        app.logger.error(f"Error in all_data_visualization route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Start Flask App + MQTT
if __name__ == '__main__':
    init_db()
    try:
        mqttClient.on_connect = on_connect
        mqttClient.on_message = on_message
        mqttClient.connect(mqttBroker, 1883, 60)
        mqttClient.loop_start()
        logging.info("MQTT client started")
    except Exception as e:
        app.logger.error(f"MQTT connection failed: {e}")
    app.run(debug=False, port=5000)