from flask_restx import fields, Api

api = Api()

registration_model_dto = api.model('RegistrationModel', {
    "id": fields.Integer(readonly=True, description="id"),
    'email': fields.String(required=True, description='User email'),
    'username': fields.String(required=True, description='Username'),
    'password': fields.String(required=True, description='Password')
})

