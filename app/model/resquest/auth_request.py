from ...extensions import api
from flask_restx import fields

registration_request_model = api.model('RegistrationRequest', {
    'email': fields.String(required=True, description='User email'),
    'username': fields.String(required=True, description='Username'),
    'password': fields.String(required=True, description='Password')
})

