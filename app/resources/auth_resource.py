import re

# import bcrypt
import psycopg2
from datetime import datetime, timedelta

import pytz
from flask_restx import Resource, Namespace, fields, reqparse
from flask_jwt_extended import create_access_token
from app.database.connect_db import conn
from passlib.hash import bcrypt
from ..extensions import api
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random

# ______  100% done for authentication ______

auth_ns = Namespace("api/v1/auth")
phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)

registration_request_model = auth_ns.model('RegistrationRequest', {
    'username': fields.String(required=True, description='Username'),
    'email': fields.String(required=True, description='User email'),
    'password': fields.String(required=True, description='Password')
})

login_model = auth_ns.model('LoginRequest', {
    'email': fields.String(required=True, description='User email'),
    'password': fields.String(required=True, description='Password')
})

otp_model = auth_ns.model('OTP', {
    'otp_code': fields.String(required=True, description='OTP Code')
})


def is_valid_email(email):
    # A more comprehensive regular expression for email validation
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    # Use re.match to check if the email matches the pattern
    if re.match(pattern, email):
        return True
    else:
        raise Exception("Email invalid")


@auth_ns.route("/register")
class Register(Resource):
    @auth_ns.expect(registration_request_model)
    def post(self):

        user = api.payload
        email = user.get('email')
        username = user.get('username')
        password = user.get('password')

        if not username:
            return {
                'success': False,
                'message': "username can't be null"
            }, 400

        if not password:
            return {'success': False, 'message': "password can't be null"}, 400

        # Validate the email from the user's payload
        try:
            if is_valid_email(email):
                print(f"{email} is a valid email.")
            else:
                return {
                    'success': False,
                    'message': 'email invalid'
                }, 400
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }, 400

        hashed_password = bcrypt.hash(password)

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM app_user WHERE email = %s OR username = %s", (email, username,))
                existing_user = cursor.fetchone()
            if existing_user:
                return {
                    'success': False,
                    'message': 'User already exists'
                }, 400

            # Create a new cursor for the user insertion
            with conn.cursor() as cur:
                query = "INSERT INTO app_user (username,email, password) VALUES (%s, %s, %s) RETURNING id;"
                cur.execute(query, (username, email, hashed_password))
                user_id = cur.fetchone()[0]
                conn.commit()
                return {
                    'date': current_datetime.isoformat(),
                    'success': True,
                    'message': 'Use register successfully',
                    'payload': {
                        'user_id': user_id, 'username': username, 'email': email
                    }
                }, 200
        except psycopg2.DatabaseError as error:
            print("DATABASE ERROR:", error)
            conn.rollback()
        except Exception as e:
            print("ERROR:", e)
            return {'response': 'An unexpected error occurred'}, 401


@auth_ns.route("/login")
class Login(Resource):
    @api.expect(login_model)
    def post(self):
        data = api.payload
        identifier = data['email']
        password = data['password']

        if not identifier:
            return {
                'success': False,
                'message': "email can't be null"
            }, 400

        if not password:
            return {
                'success': False,
                'message': "password can't be null"
            }, 400

        print("DEBUG - Identifier:", identifier)

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM app_user WHERE email = %s OR username = %s", (identifier, identifier,))
                user = cursor.fetchone()
            if not user:
                return {
                    'success': False,
                    'message': 'User not found'
                }, 404

            # Verify the password using the hashed password from the database
            if not bcrypt.verify(password, user[3]):
                return {
                    'success': False,
                    'message': 'Invalid password'
                }, 400

            # Generate and return a JWT token using the configured secret key
            access_token = create_access_token(identity=identifier)
            return {
                'date': current_datetime.isoformat(),
                'success': True,
                'message': 'login user successfully',
                'payload': {
                    'user_id': user[0],
                    'username': user[2],
                    'email': user[1],
                    'token': access_token,
                }
            }, 200

        except psycopg2.DatabaseError as error:
            return {'message': 'DATABASE ERROR'}, 400
            # print("DATABASE ERROR:", error)

        except Exception as e:
            print("ERROR:", e)
            return {'message': 'An unexpected error occurred'}, 401


email_model = auth_ns.model('Email', {
    'receiver_email': fields.String(required=True, description='Recipient email address')
})


# Create a route to send an email
@auth_ns.route('/send-email')
class SendEmail(Resource):
    @auth_ns.expect(email_model)
    def post(self):
        data = api.payload
        receiver_email = data['receiver_email']

        # Validate the email from the user's payload
        try:
            if is_valid_email(receiver_email):
                print(f"{receiver_email} is a valid email.")
            else:
                return {
                    'success': False,
                    'message': 'Email invalid'
                }, 400
        except Exception as e:
            return {
                'success': False,
                'message': str(e)
            }, 400

        sender_email = "datavue.hrd@gmail.com"
        password = "mcxpwboaomlohslp"
        subject = "OTP Code"
        otp_code = str(random.randint(1000, 9999))

        context = f"""<!DOCTYPE html>
        <html>
        <head>
        </head>
        <body style="font-family: sans-serif; font-size: 16px; background-color: #fff;">
          <table align="center" style="width: 600px; border-collapse: collapse;">
            <tr>
              <th style="text-align: center; color: #00466a; font-size: 24px; font-weight: bold;">DataVue OTP Code</th>
            </tr>
            <tr>
              <td style="padding: 20px; color: #333;">
                Hi there,
                <br>
                <br>
                Your OTP code is:
                <br>
                <br>
                <h2 style="font-size: 24px; font-weight: bold; text-align: center; color: #00466a;">{otp_code}</h2>
                <br>
                Please enter this code within the next 2 minutes to verify your account.
              </td>
            </tr>
            <tr>
              <td style="padding: 20px; text-align: center; color: #333;">
                Thanks,
                <br>
                The DataVue Team
              </td>
            </tr>
          </table>
        </body>
        </html>
        """

        # context = f"<html><body><h1>Hello</h1><p>Your OTP code is: {otp_code}</p></body></html>"
        # with open("otp_form.html", "r") as html_file:
        #     context = html_file.read()
        # context = context.replace("{otp_code}", otp_code)

        msg = MIMEMultipart()
        msg.attach(MIMEText(context, 'html'))
        msg['Subject'] = subject

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM app_user WHERE email = %s", (receiver_email,))
                user = cursor.fetchone()

            if user:
                print("existing_email", user[0], otp_code)

                server = smtplib.SMTP("smtp.gmail.com", 587)
                server.starttls()
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, msg.as_string())
                server.quit()

                with conn.cursor() as cur:

                    query = "SELECT * From upsert_otp(%s, %s,%s)"
                    current_time = datetime.now()
                    print("*current_itme", current_time)
                    cur.execute(query, (user[0], otp_code, current_time))
                    conn.commit()
                return {
                    'date': current_datetime.isoformat(),
                    'success': True,
                    'message': 'Email has been sent successfully'
                }, 200
            else:
                return {
                    'success': True,
                    "response": "User not found"
                }, 404

        except Exception as e:
            print("An error occurred:", str(e))
        finally:
            cursor.close()


otp_parser = reqparse.RequestParser()
otp_parser.add_argument('otp', type=int, required=True, help='OTP Code')


@auth_ns.route('/verify_otp')
class VerifyOTP(Resource):
    @auth_ns.expect(otp_parser)  # Specify the request parser for the 'otp' parameter
    def get(self):
        try:
            args = otp_parser.parse_args()
            receiver_otp = args['otp']

            if not receiver_otp:
                return {
                    'success': False,
                    'response': 'OTP Code is required as a request parameter.'
                }, 400

            # Query the database to retrieve the stored OTP for the user
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM user_otp WHERE code = %s", (receiver_otp,))
                user = cursor.fetchone()
                # conn.comzmit()
            print("________________ user ________________: ", user)

            if not user:
                return {'response': 'OTP verification failed. Invalid OTP code.'}, 401
            else:
                user_id = user[1]
                stored_otp = user[2]
                created_time = user[3]
                current_time = datetime.now()
                expiration_time = created_time + timedelta(minutes=3)

                print("current_time: ", current_time)
                print("expiration_time: ", expiration_time)

                if current_time <= expiration_time:
                    if receiver_otp == stored_otp:
                        return {
                                'success': True,
                                'response': 'OTP verification successful.',
                                'user_id': user_id,
                                }, 200
                    else:
                        return {
                            'success': False,
                            'response': 'OTP verification failed. Invalid OTP code.'
                        }, 400
                else:
                    return {
                        'success': False,
                        'response': 'OTP has expired.'
                    }, 400

        except Exception as e:
            return {
                'success': False,
                'response': 'An error occurred while verifying OTP.', 'error': str(e)
            }, 400


user_id_parser = reqparse.RequestParser()
user_id_parser.add_argument('user_id', type=int, required=True, help='User ID')

password_model = auth_ns.model('PasswordModel', {
    'new_password': fields.String(required=True, description='New Password')
})


@auth_ns.route("/reset_password/<int:user_id>")
class ResetPassword(Resource):
    @auth_ns.expect(password_model)
    def put(self, user_id):

        data = api.payload
        new_password = data['new_password']
        hashed_password = bcrypt.hash(new_password)
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM app_user WHERE id = %s", (user_id,))
            user_id = cursor.fetchone()
        if user_id:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE app_user SET password = %s WHERE id = %s", (hashed_password, user_id))
                conn.commit()
        else:
            return {
                'success': False,
                'message': 'User not found'
            }, 404

        return {
            'success': True,
            'message': 'Password updated successfully.'
        }, 200
