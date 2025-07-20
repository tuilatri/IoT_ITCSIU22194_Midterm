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

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Set a fixed secret key for session persistence
app.secret_key = '3427c80e7f70ff707e1031e191d47470539ad370d9481692'

# MQTT Configuration
mqttBroker = "iot-dashboard-02.cloud.shiftr.io"
mqttUser = "iot-dashboard-02"
mqttPassword = "YBxsZiVmkHljoCId"
mqttClient = mqtt.Client(client_id=f"iot-dashboard-{secrets.token_hex(8)}", protocol=mqtt.MQTTv311)  # Updated to MQTTv5

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
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id INTEGER,
            timestamp TEXT,
            humidity REAL,
            temperature REAL,
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

# MQTT Callbacks
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        app.logger.info("Connected to MQTT Broker!")
        client.subscribe("home/sensors/temperature", qos=1)
        client.subscribe("home/sensors/humidity", qos=1)
        client.subscribe("home/sensors/light", qos=1)
        client.subscribe("home/control/light", qos=1)
        client.subscribe("home/control/fan", qos=1)
        client.subscribe("home/control/motor", qos=1)
    else:
        app.logger.error(f"Failed to connect, return code {reason_code}")

# Global variables to store sensor data temporarily
latest_sensor_data = {'temperature': None, 'humidity': None, 'light': None, 'timestamp': None}
lock = threading.Lock()

def store_sensor_data(temperature=None, humidity=None, light=None):
    global latest_sensor_data
    try:
        conn = sqlite3.connect("iot_data.db")
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()
        app.logger.debug(f"Storing sensor data: T={temperature}, H={humidity}, L={light}")

        with lock:
            # Update the latest sensor data
            if temperature is not None:
                latest_sensor_data['temperature'] = temperature
                latest_sensor_data['timestamp'] = timestamp
            if humidity is not None:
                latest_sensor_data['humidity'] = humidity
                if latest_sensor_data['timestamp'] is None:
                    latest_sensor_data['timestamp'] = timestamp
            if light is not None:
                latest_sensor_data['light'] = light
                if latest_sensor_data['timestamp'] is None:
                    latest_sensor_data['timestamp'] = timestamp

            # Check if we have all sensor data and the timestamp is recent
            if (latest_sensor_data['temperature'] is not None and
                latest_sensor_data['humidity'] is not None and
                latest_sensor_data['light'] is not None and
                latest_sensor_data['timestamp'] is not None):
                # Check for duplicates within 5 seconds
                cursor.execute(
                    "SELECT temperature, humidity, light_level, timestamp FROM sensor_data WHERE device_id = ? ORDER BY id DESC LIMIT 1",
                    (1,)
                )
                last_entry = cursor.fetchone()
                if last_entry and (datetime.now() - datetime.fromisoformat(last_entry[3])).total_seconds() < 5:
                    if (last_entry[0] == latest_sensor_data['temperature'] and
                        last_entry[1] == latest_sensor_data['humidity'] and
                        last_entry[2] == latest_sensor_data['light']):
                        app.logger.debug("Skipping duplicate sensor data")
                        return

                # Insert combined sensor data
                cursor.execute(
                    "INSERT INTO sensor_data (device_id, timestamp, temperature, humidity, light_level) VALUES (?, ?, ?, ?, ?)",
                    (1, latest_sensor_data['timestamp'], latest_sensor_data['temperature'],
                     latest_sensor_data['humidity'], latest_sensor_data['light'])
                )
                app.logger.debug(f"Inserted combined sensor data: T={latest_sensor_data['temperature']}, "
                               f"H={latest_sensor_data['humidity']}, L={latest_sensor_data['light']}")
                # Reset latest sensor data
                latest_sensor_data = {'temperature': None, 'humidity': None, 'light': None, 'timestamp': None}

        conn.commit()
        app.logger.info(f"Stored sensor data: T={temperature}, H={humidity}, L={light}")
    except Exception as e:
        app.logger.error(f"Error in store_sensor_data: {e}")
        conn.rollback()
    finally:
        conn.close()

def on_mqtt_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    app.logger.info(f"Received {payload} from {topic}")
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
            store_motor_status(payload)
        app.logger.debug(f"[✓] Processed: {payload} from {topic}")
    except Exception as e:
        app.logger.error(f"[✗] Failed to process message from {topic}: {e}")

# Publish sensor data periodically
def publish_sensor_data():
    while True:
        try:
            conn = sqlite3.connect('iot_data.db')
            cursor = conn.cursor()
            cursor.execute("SELECT humidity, temperature, light_level FROM sensor_data WHERE humidity IS NOT NULL AND temperature IS NOT NULL AND light_level IS NOT NULL ORDER BY id DESC LIMIT 1")
            sensor_data = cursor.fetchone()
            conn.close()
            if sensor_data:
                payload = f"{sensor_data[0]},{sensor_data[1]},{sensor_data[2]}"
                mqttClient.publish("home/sensors", payload, qos=1)
                app.logger.debug(f"[✓] Published sensor data: {payload}")
            else:
                app.logger.warning("No complete sensor data to publish")
        except Exception as e:
            app.logger.error(f"Error publishing sensor data: {e}")
        time.sleep(60)

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
        app.logger.info(f"Stored light status: {action} for device {device_id}")
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
        app.logger.info(f"Stored fan status: {action}")
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
            if last_entry[0].lower() == direction.lower():
                app.logger.debug(f"Skipping duplicate motor status: {direction}")
                return
        cursor.execute(
            "INSERT INTO dc_motor_status (device_id, timestamp, direction) VALUES (?, ?, ?)",
            (3, datetime.now().isoformat(), direction.upper())
        )
        conn.commit()
        app.logger.info(f"Stored motor status: {direction}")
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
    if action not in ['ON', 'OFF']:
        return jsonify({'status': 'error', 'message': 'Invalid fan action'}), 400
    try:
        mqttClient.publish("home/control/fan", action, qos=1)
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
    if action not in ['FORWARD', 'BACKWARD', 'STOP']:
        return jsonify({'status': 'error', 'message': 'Invalid motor action'}), 400
    try:
        mqttClient.publish("home/control/motor", action, qos=1)
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
        cursor.execute("SELECT humidity, temperature, light_level FROM sensor_data WHERE humidity IS NOT NULL AND temperature IS NOT NULL AND light_level IS NOT NULL ORDER BY id DESC LIMIT 1")
        sensor_data = cursor.fetchone()
        conn.close()
        return jsonify({
            'humidity': sensor_data[0] if sensor_data else None,
            'temperature': sensor_data[1] if sensor_data else None,
            'light': sensor_data[2] if sensor_data else None
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
        cursor.execute("SELECT id, timestamp, humidity, temperature, light_level FROM sensor_data ORDER BY timestamp DESC LIMIT 100")
        sensor_data = cursor.fetchall()
        
        # Structure sensor data
        sensors = [{
            'id': row[0],
            'timestamp': row[1],
            'humidity': row[2],
            'temperature': row[3],
            'light_level': row[4]
        } for row in sensor_data]
        
        # Fetch LED, fan, and motor data
        cursor.execute("SELECT id, timestamp, status FROM relay_lights_status WHERE device_id = 1 ORDER BY timestamp DESC LIMIT 100")
        led1 = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, status FROM relay_fans_status WHERE device_id = 6 ORDER BY timestamp DESC LIMIT 100")
        fan = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, direction FROM dc_motor_status WHERE device_id = 3 ORDER BY timestamp DESC LIMIT 100")
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
        cursor.execute("SELECT id, timestamp, humidity, temperature, light_level FROM sensor_data ORDER BY timestamp DESC LIMIT 100")
        sensor_data = cursor.fetchall()
        
        # Structure sensor data
        sensors = [{
            'id': row[0],
            'timestamp': row[1],
            'humidity': row[2],
            'temperature': row[3],
            'light_level': row[4]
        } for row in sensor_data]
        
        # Fetch LED, fan, and motor data
        cursor.execute("SELECT id, timestamp, status FROM relay_lights_status WHERE device_id = 1 ORDER BY timestamp DESC LIMIT 100")
        led1 = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, status FROM relay_fans_status WHERE device_id = 6 ORDER BY timestamp DESC LIMIT 100")
        fan = [{'id': row[0], 'timestamp': row[1], 'status': row[2]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT id, timestamp, direction FROM dc_motor_status WHERE device_id = 3 ORDER BY timestamp DESC LIMIT 100")
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
        mqttClient.on_message = on_mqtt_message
        mqttClient.connect(mqttBroker, 1883, 60)
        mqttClient.loop_start()
        threading.Thread(target=publish_sensor_data, daemon=True).start()
        app.logger.info("MQTT client started")
    except Exception as e:
        app.logger.error(f"MQTT connection failed: {e}")
    app.run(debug=True, host='0.0.0.0', port=5000)