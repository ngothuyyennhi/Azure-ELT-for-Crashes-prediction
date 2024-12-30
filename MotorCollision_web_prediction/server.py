from flask import Flask, request, jsonify
from azure.identity import DefaultAzureCredential
from azure.eventhub import EventHubProducerClient, EventData
from flask_cors import CORS
import json
import pandas as pd
import requests


app = Flask(__name__)
CORS(app)  # Kích hoạt CORS cho Flask

# Thông tin Event Hub
event_hubs_resource_name = "DoAnCuoiKy"
fully_qualified_namespace = f"{event_hubs_resource_name}.servicebus.windows.net"
event_hub_name = "doancuoiki_eventhub"
DATABRICKS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")

# Cấu hình xác thực Azure
credential = DefaultAzureCredential()

@app.route('/send-to-event-hub', methods=['POST'])
def send_to_event_hub():
    # Nhận dữ liệu từ body của request
    data = request.json
    required_fields = [
        "contributing_factor_vehicle_1",
        "contributing_factor_vehicle_2",
        "contributing_factor_vehicle_3",
        "contributing_factor_vehicle_4",
        "contributing_factor_vehicle_5",
        "vehicle_type_code1",
        "vehicle_type_code2",
        "borough",
    ]
    
    # Kiểm tra các trường bắt buộc
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Thiếu thông tin bắt buộc trong yêu cầu."}), 400

    try:
        # Tạo producer client
        producer = EventHubProducerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=event_hub_name,
            credential=credential
        )

        # Tạo batch sự kiện
        with producer:
            event_data_batch = producer.create_batch()

            # Thêm dữ liệu vào batch
            event_data = EventData(body=json.dumps(data).encode('utf-8'))
            event_data_batch.add(event_data)

            # Gửi batch đến Event Hub
            producer.send_batch(event_data_batch)

        print("Dữ liệu đã được gửi đến Azure Event Hub.")
        return jsonify({"message": "Dữ liệu đã được gửi thành công!"}), 200

    except Exception as e:
        print("Error sending data to Event Hub:", e)
        return jsonify({"error": "Gửi dữ liệu thất bại."}), 500

# Dữ liệu ánh xạ mã hóa ban đầu (giống cách StringIndexer hoạt động)
string_indexers = {'borough': {'b': 2,
             'Bronx': 4,
             'Brooklyn': 0,
             'Manhattan': 3,
             'Queens': 1,
             'Staten Island': 5},
 'contributing_factor_vehicle_1': {'Aggressive Driving/Road Rage': 21,
                                   'Alcohol Involvement': 9,
                                   'Animals Action': 26,
                                   'Backing Unsafely': 11,
                                   'Brakes Defective': 19,
                                   'Driver Inattention/Distraction': 1,
                                   'Driver Inexperience': 10,
                                   'Drugs (illegal)': 31,
                                   'Eating or Drinking': 32,
                                   'Failure to Keep Right': 33,
                                   'Failure to Yield Right-of-Way': 2,
                                   'Fatigued/Drowsy': 27,
                                   'Fell Asleep': 20,
                                   'Following Too Closely': 3,
                                   'Illnes': 22,
                                   'Lost Consciousness': 23,
                                   'Obstruction/Debris': 29,
                                   'Other Lighting Defects': 34,
                                   'Other Vehicular': 7,
                                   'Outside Car Distraction': 30,
                                   'Oversized Vehicle': 28,
                                   'Passenger Distraction': 24,
                                   'Passing Too Closely': 6,
                                   'Passing or Lane Usage Improper': 4,
                                   'Pavement Defective': 35,
                                   'Pavement Slippery': 12,
                                   'Pedestrian/Bicyclist/Other Pedestrian Error/Confusion': 14,
                                   'Reaction to Uninvolved Vehicle': 18,
                                   'Steering Failure': 36,
                                   'Tire Failure/Inadequate': 25,
                                   'Traffic Control Device Improper/Non-Working': 37,
                                   'Traffic Control Disregarded': 8,
                                   'Turning Improperly': 15,
                                   'U': 17,
                                   'Unsafe Lane Changing': 13,
                                   'Unsafe Speed': 5,
                                   'Unspecified': 0,
                                   'View Obstructed/Limited': 16},
 'contributing_factor_vehicle_2': {'Aggressive Driving/Road Rage': 18,
                                   'Alcohol Involvement': 14,
                                   'Backing Unsafely': 15,
                                   'Driver Inattention/Distraction': 2,
                                   'Driver Inexperience': 9,
                                   'Failure to Yield Right-of-Way': 5,
                                   'Following Too Closely': 3,
                                   'Other Vehicular': 6,
                                   'Outside Car Distraction': 19,
                                   'Oversized Vehicle': 20,
                                   'Passing Too Closely': 8,
                                   'Passing or Lane Usage Improper': 16,
                                   'Pavement Slippery': 4,
                                   'Pedestrian/Bicyclist/Other Pedestrian Error/Confusion': 11,
                                   'Traffic Control Device Improper/Non-Working': 21,
                                   'Traffic Control Disregarded': 7,
                                   'Turning Improperly': 12,
                                   'U': 1,
                                   'Unsafe Lane Changing': 17,
                                   'Unsafe Speed': 13,
                                   'Unspecified': 0,
                                   'View Obstructed/Limited': 10},
 'contributing_factor_vehicle_3': {'Driver Inattention/Distraction': 1,
                                   'Pavement Slippery': 2,
                                   'Unsafe Speed': 3,
                                   'Unspecified': 0},
 'contributing_factor_vehicle_4': {'Pavement Slippery': 1, 'Unspecified': 0},
 'contributing_factor_vehicle_5': {'Unspecified': 0},
 'vehicle_type_code1': {'AMBULANCE': 18,
                        'Ambulance': 10,
                        'Beverage Truck': 19,
                        'Bike': 11,
                        'Box Truck': 6,
                        'Bus': 5,
                        'Dump': 12,
                        'E-Bike': 13,
                        'E-Scooter': 14,
                        'Garbage or Refuse': 15,
                        'Moped': 16,
                        'Motorcycle': 8,
                        'Motorscooter': 20,
                        'PK': 17,
                        'Pick up tr': 21,
                        'Pick-up Truck': 4,
                        'S': 2,
                        'Sedan': 0,
                        'Station Wagon/Sport Utility Vehicle': 1,
                        'Taxi': 3,
                        'Tow Truck / Wrecker': 22,
                        'Tractor Truck Diesel': 9,
                        'Tractor Truck Gasoline': 23,
                        'UNICYCLE': 24,
                        'UNK': 25,
                        'VAN BUS': 26,
                        'Van': 7,
                        'a': 27,
                        'd': 28,
                        'd1': 29,
                        'subn': 30,
                        'van': 31},
 'vehicle_type_code2': {'Ambulance': 12,
                        'BOX TRUCK': 17,
                        'Bike': 3,
                        'Box Truck': 4,
                        'Bus': 6,
                        'C1': 18,
                        'Carry All': 13,
                        'Con/a': 19,
                        'Concrete Mixer': 20,
                        'Convertible': 21,
                        'Delivery T': 22,
                        'E-Bike': 10,
                        'E-Scooter': 11,
                        'FDNY FIRE': 23,
                        'Flat Bed': 24,
                        'Flat Rack': 25,
                        'Garbage or Refuse': 26,
                        'Grater': 27,
                        'LIMO': 28,
                        'Lunch Wagon': 29,
                        'MTA BUS': 30,
                        'Moped': 8,
                        'Motorbike': 14,
                        'Motorcycle': 15,
                        'PK': 31,
                        'Pick-up Truck': 5,
                        'Sedan': 1,
                        'Station Wagon/Sport Utility Vehicle': 2,
                        'TRUCK': 32,
                        'Tanker': 16,
                        'Taxi': 7,
                        'Tractor Truck Diesel': 33,
                        'Unspecified': 0,
                        'Van': 9,
                        'nissa': 34,
                        'subn': 35,
                        'van': 36}}

def create_string_indexer(dataframe, columns):
    """
    Tạo ánh xạ StringIndexer cho các cột và lưu kết quả vào string_indexers.
    """
    for column in columns:
        unique_values = dataframe[column].dropna().unique()
        string_indexers[column] = {value: idx for idx, value in enumerate(unique_values)}

def encode_dataframe(dataframe, columns):
    """
    Sử dụng ánh xạ trong string_indexers để mã hóa các cột của DataFrame.
    """
    for column in columns:
        if column in string_indexers:
            dataframe[column + "_encoded"] = dataframe[column].map(string_indexers[column])
        else:
            dataframe[column + "_encoded"] = -1
    return dataframe

@app.route('/predict', methods=['POST'])
def predict():
    # Nhận dữ liệu từ client
    try:
        raw_data = request.json
        print("raw data: ", raw_data)
        input_df = pd.DataFrame([raw_data])  # Chuyển dữ liệu JSON thành DataFrame
        print(input_df)
    except Exception as e:
        return jsonify({"error": "Invalid input format hehe"}), 400

    # Các cột cần mã hóa
    categorical_cols = [
        "borough",
        "contributing_factor_vehicle_1", 
        "contributing_factor_vehicle_2",
        "contributing_factor_vehicle_3",
        "contributing_factor_vehicle_4",
        "contributing_factor_vehicle_5",
        "vehicle_type_code1", 
        "vehicle_type_code2"
    ]

    # Tạo ánh xạ nếu chưa có
    if not string_indexers:
        create_string_indexer(input_df, categorical_cols)

    # Mã hóa dữ liệu
    try:
        encoded_df = encode_dataframe(input_df, categorical_cols)
    except Exception as e:
        return jsonify({"error": "Encoding failed", "details": str(e)}), 500
    print("Brooklyn" in string_indexers['borough'])  # Kiểm tra trực tiếp

    # Chuẩn bị dữ liệu JSON gửi đến API model
    inputs = encoded_df[[
        col + "_encoded" for col in categorical_cols
    ]].to_dict(orient="records")
    model_request_payload = {"inputs": inputs}
    print(model_request_payload)

    # Gửi request đến API model
    url = 'https://adb-4065452090489326.6.azuredatabricks.net/model/random_forest_model/3/invocations'
    headers = {
        'Authorization': 'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, json=model_request_payload)
        print(response.json())
        if response.status_code != 200:
            return jsonify({"error": f"Model API error: {response.status_code}", "details": response.text}), 500
        return jsonify(response.json()), 200
    except Exception as e:
        return jsonify({"error": "Error calling model API", "details": str(e)}), 500

# Khởi chạy server
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
