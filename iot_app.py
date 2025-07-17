from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import paho.mqtt.client as mqtt
import sqlite3
from datetime import datetime
import logging
import time
import threading
from werkzeug.security import generate_password_hash, check_password_hash
import secrets

app = Flask(__name__)

# Configure logging to ensure console output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Ensure logs go to console
    ]
)

# Set a fixed secret key for session persistence
app.secret_key = '3427c80e7f70ff707e1031e191d47470539ad370d9481692'  # Replace with a secure key

# MQTT Configuration
mqttBroker = "iot-dashboard.cloud.shiftr.io"
mqttUser = "iot-dashboard"
mqttPassword = "YBxsZiVmkHljoCId"
mqttClient = mqtt.Client(client_id="", protocol=mqtt.MQTTv5)

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
def on_mqtt_message(client, userdata, msg):
    if msg.topic == "home/sensors":
        try:
            payload = msg.payload.decode()
            humidity, temperature, light = map(float, payload.split(","))
            store_sensor_data(humidity, temperature, light)
            app.logger.debug(f"[✓] Stored: H={humidity}, T={temperature}, L={light}")
        except Exception as e:
            app.logger.error(f"[✗] Failed to process sensor data: {e}")

# Publish sensor data periodically
def publish_sensor_data():
    while True:
        try:
            conn = sqlite3.connect('iot_data.db')
            cursor = conn.cursor()
            cursor.execute("SELECT humidity, temperature FROM temperature_humidity_data ORDER BY id DESC LIMIT 1")
            temp_hum = cursor.fetchone()
            cursor.execute("SELECT light_level FROM light_sensor_data ORDER BY id DESC LIMIT 1")
            light = cursor.fetchone()
            conn.close()
            
            if temp_hum and light:
                payload = f"{temp_hum[0]},{temp_hum[1]},{light[0]}"
                mqttClient.publish("home/sensors", payload)
                app.logger.debug(f"[✓] Published sensor data: {payload}")
        except Exception as e:
            app.logger.error(f"Error publishing sensor data: {e}")
        time.sleep(60)  # Publish every 60 seconds

# Database Functions
def store_sensor_data(humidity, temperature, light):
    try:
        conn = sqlite3.connect("iot_data.db")
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()
        cursor.execute(
            "INSERT INTO temperature_humidity_data (device_id, timestamp, humidity, temperature) VALUES (?, ?, ?, ?)",
            (1, timestamp, humidity, temperature)
        )
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
            "INSERT INTO relay_lights_status (device_id, timestamp, status) VALUES (?, ?, ?)",
            (device_id, datetime.now().isoformat(), 1 if action.endswith('_on') else 0)
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
            "INSERT INTO relay_fans_status (device_id, timestamp, status) VALUES (?, ?, ?)",
            (6, datetime.now().isoformat(), 1 if action == 'on' else 0)
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
    if action not in ['led1_on', 'led1_off'] or not device_id:
        return jsonify({'status': 'error', 'message': 'Invalid action or device_id'}), 400
    try:
        mqttClient.publish("home/control", action)
        store_light_status(action, device_id)
        return jsonify({'status': 'success'})
    except Exception as e:
        app.logger.error(f"Error in control_led route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/control_fan', methods=['POST'])
def control_fan():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    action = request.json.get('action')
    if action not in ['on', 'off']:
        return jsonify({'status': 'error', 'message': 'Invalid fan action'}), 400
    try:
        mqttClient.publish("home/control", f"fan_{action}")
        store_fan_status(action)
        return jsonify({'status': 'success'})
    except Exception as e:
        app.logger.error(f"Error in control_fan route: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/control_motor', methods=['POST'])
def control_motor():
    if 'user_id' not in session:
        return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
    action = request.json.get('action')
    if action not in ['forward', 'backward', 'stop']:
        return jsonify({'status': 'error', 'message': 'Invalid motor action'}), 400
    try:
        mqttClient.publish("home/control", f"motor_{action}")
        store_motor_status(action)
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
        
        # Fetch sensor data
        cursor.execute("SELECT id, timestamp, humidity, temperature FROM temperature_humidity_data ORDER BY timestamp DESC")
        temp_hum_data = cursor.fetchall()
        cursor.execute("SELECT id, timestamp, light_level FROM light_sensor_data ORDER BY timestamp DESC")
        light_data = cursor.fetchall()
        
        # Combine sensor data by timestamp
        sensors = []
        for th, l in zip(temp_hum_data, light_data):
            sensors.append({
                'id': th[0],
                'timestamp': th[1],
                'humidity': th[2],
                'temperature': th[3],
                'light_level': l[2]
            })
        
        # Fetch LED, fan, and motor data
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
        
        # Fetch sensor data
        cursor.execute("SELECT id, timestamp, humidity, temperature FROM temperature_humidity_data ORDER BY timestamp DESC")
        temp_hum_data = cursor.fetchall()
        cursor.execute("SELECT id, timestamp, light_level FROM light_sensor_data ORDER BY timestamp DESC")
        light_data = cursor.fetchall()
        
        # Combine sensor data by timestamp
        sensors = []
        for th, l in zip(temp_hum_data, light_data):
            sensors.append({
                'id': th[0],
                'timestamp': th[1],
                'humidity': th[2],
                'temperature': th[3],
                'light_level': l[2]
            })
        
        # Fetch LED, fan, and motor data
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
        mqttClient.on_message = on_mqtt_message
        mqttClient.connect(mqttBroker, 1883, 60)
        mqttClient.subscribe("home/sensors")
        mqttClient.loop_start()
        threading.Thread(target=publish_sensor_data, daemon=True).start()
    except Exception as e:
        app.logger.error(f"MQTT connection failed: {e}")
    app.run(debug=True)