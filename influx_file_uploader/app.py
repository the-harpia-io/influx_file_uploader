import imghdr
import os
from flask import Flask, render_template, request, redirect, url_for, abort, \
    send_from_directory
from werkzeug.utils import secure_filename
from influx_file_uploader.logic.csv_to_influx import converter

# app = Flask(__name__, template_folder='./template')
app = Flask(__name__, template_folder='/code/influx_file_uploader/template')
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024
app.config['UPLOAD_EXTENSIONS'] = ['.csv']
app.config['UPLOAD_PATH'] = './'


def validate_image(stream):
    header = stream.read(512)
    stream.seek(0)
    format = imghdr.what(None, header)
    if not format:
        return None
    return '.' + (format if format != 'jpeg' else 'jpg')


@app.errorhandler(413)
def too_large(e):
    return "File is too large", 413


@app.route('/')
def index():
    files = os.listdir(app.config['UPLOAD_PATH'])
    return render_template('index.html', files=files)


@app.route('/', methods=['POST'])
def upload_files():
    my_files = request.files
    device_name = request.form['Device Name']
    road_name = request.form['Road Name']
    tags = {'device_name': device_name, 'road_name': road_name}

    for item in my_files:
        uploaded_file = my_files.get(item)
        filename = secure_filename(uploaded_file.filename)
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext not in app.config['UPLOAD_EXTENSIONS']:
                return "Invalid Format", 400
            decoded_file = uploaded_file.read().decode('utf-8')
            data = decoded_file.split('\n')

            header = data.pop(0).split(';')[:-1]

            full_data = []
            for single_row in data:
                single_result = single_row.split(';')[:-1]
                enrich_result = dict(zip(header, single_result))
                enrich_result_with_tags = {**enrich_result, **tags}
                full_data.append(enrich_result_with_tags)

            converter(full_data)

    return '', 204


@app.route('/uploads/<filename>')
def upload(filename):
    return send_from_directory(app.config['UPLOAD_PATH'], filename)


def main():
    app.run(debug=True, port=8080, host='0.0.0.0')


if __name__ == '__main__':
    main()
