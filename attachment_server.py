# Simple web server to serve email attachments
from flask import Flask, send_file, abort, request, jsonify
import os
import mimetypes
from werkzeug.security import safe_join

app = Flask(__name__)

# Configuration
ATTACHMENT_BASE_PATH = os.getenv("ATTACHMENT_STORE_PATH", r"D:\attachments")
ALLOWED_EXTENSIONS = {'.pdf', '.docx', '.doc', '.xlsx', '.xls', '.txt', '.png', '.jpg', '.jpeg', '.gif', '.zip', '.rar'}

def is_safe_path(path):
    return os.path.commonpath([os.path.realpath(path), os.path.realpath(ATTACHMENT_BASE_PATH)]) == os.path.realpath(ATTACHMENT_BASE_PATH)

@app.route('/attachments/<user_name>/<email_id>/<filename>')
def serve_attachment(user_name, email_id, filename):
    try:
        # Construct safe file path
        file_path = safe_join(ATTACHMENT_BASE_PATH, user_name, email_id, filename)
        
        if not file_path or not os.path.exists(file_path):
            abort(404)
        
        # Security check
        if not is_safe_path(file_path):
            abort(403)
        
        # Check file extension
        _, ext = os.path.splitext(filename)
        if ext.lower() not in ALLOWED_EXTENSIONS:
            abort(403)
        
        # Get MIME type
        mime_type, _ = mimetypes.guess_type(filename)
        if not mime_type:
            mime_type = 'application/octet-stream'
        
        return send_file(file_path, mimetype=mime_type, as_attachment=False)
        
    except Exception as e:
        app.logger.error(f"Error serving attachment: {e}")
        abort(500)

@app.route('/attachments/<user_name>/<email_id>/<filename>/download')
def download_attachment(user_name, email_id, filename):
    try:
        file_path = safe_join(ATTACHMENT_BASE_PATH, user_name, email_id, filename)
        
        if not file_path or not os.path.exists(file_path):
            abort(404)
        
        if not is_safe_path(file_path):
            abort(403)
        
        return send_file(file_path, as_attachment=True, download_name=filename)
        
    except Exception as e:
        app.logger.error(f"Error downloading attachment: {e}")
        abort(500)

@app.route('/attachments/<user_name>/<email_id>/<filename>/info')
def attachment_info(user_name, email_id, filename):
    try:
        file_path = safe_join(ATTACHMENT_BASE_PATH, user_name, email_id, filename)
        
        if not file_path or not os.path.exists(file_path):
            abort(404)
        
        if not is_safe_path(file_path):
            abort(403)
        
        stat = os.stat(file_path)
        mime_type, _ = mimetypes.guess_type(filename)
        
        return jsonify({
            'filename': filename,
            'size': stat.st_size,
            'mime_type': mime_type,
            'modified': stat.st_mtime
        })
        
    except Exception as e:
        app.logger.error(f"Error getting attachment info: {e}")
        abort(500)

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)


# minio.exe server C:\minio\data --address ":9000" --console-address ":9001" to run 9001 port
