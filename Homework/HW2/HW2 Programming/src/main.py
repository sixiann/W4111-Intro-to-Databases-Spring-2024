from typing import Any, Dict

# Simple starter project to test installation and environment.
# Based on https://fastapi.tiangolo.com/tutorial/first-steps/
from fastapi import FastAPI, Response, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
# Explicitly included uvicorn to enable starting within main program.
# Starting within main program is a simple way to enable running
# the code within the PyCharm debugger
import uvicorn

from db import DB

# Type definitions
KV = Dict[str, Any]  # Key-value pairs

app = FastAPI()

# NOTE: In a prod environment, never put this information in code!
# There are design patterns for passing confidential information to
# application.
# TODO: You may need to change the password
db = DB(
	host="localhost",
	port=3306,
	user="root",
	password="dbuserdbuser",
	database="s24_hw2",
)

@app.get("/")
async def healthcheck():
	return HTMLResponse(content="<h1>Heartbeat</h1>", status_code=status.HTTP_200_OK)


# TODO: all methods below

# --- STUDENTS ---

@app.get("/students")
async def get_students(req: Request):
	"""Gets all students that satisfy the specified query parameters.

	GET http://0.0.0.0:8002/students?email=dff9@columbia.edu&iq=50

	select * from students where email='dff9@columbia.edu' and iq='50'

	GET http://0.0.0.0:8002/students?email=dff9@columbia.edu&iq=50&fields=last_name,iq

	select last_name, iq from students where email='dff9@columbia.edu' and iq='50'

	For instance,
		GET http://0.0.0.0:8002/students
	should return all attributes for all students.

	For instance,
		GET http://0.0.0.0:8002/students?first_name=John&last_name=Doe
	should return all attributes for students whose first name is John and last name is Doe.

	You must implement a special query parameter, `fields`. You can assume
	`fields` is a comma-separated list of attribute names. For instance,
		GET http://0.0.0.0:8002/students?first_name=John&fields=first_name,email
	should return the first name and email for students whose first name is John.
	Not every request will have a `fields` parameter.

	You can assume the query parameters are valid attribute names in the student table
	(except `fields`).

	:param req: The request that optionally contains query parameters
	:returns: A list of dicts representing students. The HTTP status should be set to 200 OK.
	"""

	# Use `dict(req.query_params)` to access query parameters
<<<<<<< HEAD
	# Convert query parameters to a dictionary
	query_params = dict(req.query_params)

	fields = query_params.pop('fields', None)
	if fields:
		fields = fields.split(',')

	rows = db.select('student', fields, query_params)

	return JSONResponse(content=rows, status_code=status.HTTP_200_OK)
=======

	try:
		# raise NotImplemented()
		result = [
			{"cat": True},
			{"canary": "Bird"}
		]
	except Exception as e:
		response = JSONResponse(content=result, status_code=200)

>>>>>>> 9c8b2d1f4a2c2807025909df0d432b6eb58b0fc0

@app.get("/students/{student_id}")
async def get_student(student_id: int):
	"""Gets a student by ID.

	For instance,
		GET http://0.0.0.0:8002/students/1
	should return the student with student ID 1. The returned value should
	be a dict-like object, not a list.

	If the student ID doesn't exist, the HTTP status should be set to 404 Not Found.

	:param student_id: The ID to be matched
	:returns: If the student ID exists, a dict representing the student with HTTP status set to 200 OK.
				If the student ID doesn't exist, the HTTP status should be set to 404 Not Found.
	"""
	filters = {'student_id': student_id}
	students = db.select('student', None, filters)

	if students:
		return JSONResponse(content=students[0], status_code=status.HTTP_200_OK)
	else:
		return JSONResponse(content="",status_code=status.HTTP_404_NOT_FOUND)
	

@app.post("/students")
async def post_student(req: Request):
	"""Creates a student.

	You can assume the body of the POST request is a JSON object that represents a student.
	You can assume the request body contains only attributes found in the student table and does not
	attempt to set `student_id`.

	For instance,
		POST http://0.0.0.0:8002/students
		{
			"first_name": "John",
			"last_name": "Doe",
			...
		}
	should create a student with the attributes specified in the request body.

	If the email is not specified in the request body, the HTTP status should be set to 400 Bad Request.
	If the email already exists in the student table, the HTTP status should be set to 400 Bad Request.
	If the enrollment year is not valid, the HTTP status should be set to 400 Bad Request.

	:param req: The request, which contains a student JSON in its body
	:returns: If the request is valid, the HTTP status should be set to 201 Created.
				If the request is not valid, the HTTP status should be set to 400 Bad Request.
	"""

	# Use `await req.json()` to access the request body
	student_data = await req.json()

	bad_request = JSONResponse(content="bad request",status_code=status.HTTP_400_BAD_REQUEST)
	

	if 'email' not in student_data:
		return bad_request
	
	existing_students = db.select('student', None, {'email': student_data['email']})
	if existing_students:
		return bad_request
	
	if 'enrollment_year' in student_data:
		try:
			year = int(student_data['enrollment_year'])
			if year < 2016 or year > 2023: 
				return bad_request
		except:
			return bad_request

	try:
		n_rows = db.insert('student', student_data)
		if n_rows == 0:
			return bad_request
		
		return JSONResponse(content="Success", status_code=status.HTTP_201_CREATED)
	except:
		return bad_request


	



@app.put("/students/{student_id}")
async def put_student(student_id: int, req: Request):
	"""Updates a student.

	You can assume the body of the PUT request is a JSON object that represents a student.
	You can assume the request body contains only attributes found in the student table and does not
	attempt to update `student_id`.

	For instance,
		PUT http://0.0.0.0:8002/students/1
		{
			"first_name": "Joe"
		}
	should update the student with student ID 1's first name to Joe.

	If the student does not exist, the HTTP status should be set to 404 Not Found.
	If the email is set to null in the request body, the HTTP status should be set to 400 Bad Request.
	If the email already exists in the student table, the HTTP status should be set to 400 Bad Request.
	If the enrollment year is not valid, the HTTP status should be set to 400 Bad Request.

	:param student_id: The ID of the student to be updated
	:param req: The request, which contains a student JSON in its body
	:returns: If the request is valid, the HTTP status should be set to 200 OK.
				If the request is not valid, the HTTP status should be set to the appropriate error code.
	"""

	# Use `await req.json()` to access the request body
	student_data = await req.json()
	bad_request = JSONResponse(content="bad request",status_code=status.HTTP_400_BAD_REQUEST)

	filters = {'student_id': student_id}
	student = db.select('student', None, filters)
	if not student: 
		return JSONResponse(content="",status_code=status.HTTP_404_NOT_FOUND)
	
	if 'email' in student_data:
		if student_data['email'] == None:
			return bad_request 

		existing_students = db.select('student', None, {'email': student_data['email']})
		if existing_students:
			return bad_request
	
	if 'enrollment_year' in student_data:
		try:
			year = int(student_data['enrollment_year'])
			if year < 2016 or year > 2023:
				return bad_request
		except:
			return bad_request
	
	try:
		n_rows = db.update('student', student_data, filters)
		if n_rows == 0:
			return bad_request
		
		return JSONResponse(content="Success", status_code=status.HTTP_200_OK)
	except:
		return bad_request
	


@app.delete("/students/{student_id}")
async def delete_student(student_id: int):
	"""Deletes a student.

	For instance,
		DELETE http://0.0.0.0:8002/students/1
	should delete the student with student ID 1.

	If the student does not exist, the HTTP status should be set to 404 Not Found.

	:param student_id: The ID of the student to be deleted
	:returns: If the request is valid, the HTTP status should be set to 200 OK.
				If the request is not valid, the HTTP status should be set to 404 Not Found.
	"""
	filters = {'student_id': student_id}
	student = db.select('student', None, filters)
	if not student: 
		return JSONResponse(content="",status_code=status.HTTP_404_NOT_FOUND)
	
	bad_request = JSONResponse(content="bad request",status_code=status.HTTP_400_BAD_REQUEST)

	try:
		n_rows = db.delete('student', filters)
		if n_rows == 0:
			return bad_request
		
		return JSONResponse(content="Success", status_code=status.HTTP_200_OK)
	except:
		return bad_request
	



# --- EMPLOYEES ---

@app.get("/employees")
async def get_employees(req: Request):
	"""Gets all employees that satisfy the specified query parameters.

	For instance,
		GET http://0.0.0.0:8002/employees
	should return all attributes for all employees.

	For instance,
		GET http://0.0.0.0:8002/employees?first_name=Don&last_name=Ferguson
	should return all attributes for employees whose first name is Don and last name is Ferguson.

	You must implement a special query parameter, `fields`. You can assume
	`fields` is a comma-separated list of attribute names. For instance,
		GET http://0.0.0.0:8002/employees?first_name=Don&fields=first_name,email
	should return the first name and email for employees whose first name is Don.
	Not every request will have a `fields` parameter.

	You can assume the query parameters are valid attribute names in the employee table
	(except `fields`).

	:param req: The request that optionally contains query parameters
	:returns: A list of dicts representing employees. The HTTP status should be set to 200 OK.
	"""

	# Use `dict(req.query_params)` to access query parameters
	query_params = dict(req.query_params)

	fields = query_params.pop('fields', None)
	if fields:
		fields = fields.split(',')

	rows = db.select('employee', fields, query_params)

	return JSONResponse(content=rows, status_code=status.HTTP_200_OK)

@app.get("/employees/{employee_id}")
async def get_employee(employee_id: int):
	"""Gets an employee by ID.

	For instance,
		GET http://0.0.0.0:8002/employees/1
	should return the employee with employee ID 1. The returned value should
	be a dict-like object, not a list.

	If the employee ID doesn't exist, the HTTP status should be set to 404 Not Found.

	:param employee_id: The ID to be matched
	:returns: If the employee ID exists, a dict representing the employee with HTTP status set to 200 OK.
				If the employee ID doesn't exist, the HTTP status should be set to 404 Not Found.
	"""
	filters = {'employee_id': employee_id}
	employees = db.select('employee', None, filters)

	if employees:
		return JSONResponse(content=employees[0], status_code=status.HTTP_200_OK)
	else:
		return JSONResponse(content="",status_code=status.HTTP_404_NOT_FOUND)

@app.post("/employees")
async def post_employee(req: Request):
	"""Creates an employee.

	You can assume the body of the POST request is a JSON object that represents an employee.
	You can assume the request body contains only attributes found in the employee table and does not
	attempt to set `employee_id`.

	For instance,
		POST http://0.0.0.0:8002/employees
		{
			"first_name": "Don",
			"last_name": "Ferguson",
			...
		}
	should create an employee with the attributes specified in the request body.

	If the email is not specified in the request body, the HTTP status should be set to 400 Bad Request.
	If the email already exists in the employee table, the HTTP status should be set to 400 Bad Request.
	If the employee type is not valid, the HTTP status should be set to 400 Bad Request.

	:param req: The request, which contains an employee JSON in its body
	:returns: If the request is valid, the HTTP status should be set to 201 Created.
				If the request is not valid, the HTTP status should be set to 400 Bad Request.
	"""

	# Use `await req.json()` to access the request body
	employee_data = await req.json()

	bad_request = JSONResponse(content="bad request",status_code=status.HTTP_400_BAD_REQUEST)
	

	if 'email' not in employee_data:
		return bad_request
	
	existing_employees = db.select('employee', None, {'email': employee_data['email']})
	if existing_employees:
		return bad_request
	
	if 'employee_type' not in employee_data:
		return bad_request
	
	
	try:
		employee_type = employee_data['employee_type']
		if employee_type not in ['Professor', 'Lecturer', 'Staff']:
			return bad_request
	except:
		bad_request
	

	try:
		n_rows = db.insert('employee', employee_data)
		if n_rows == 0:
			return bad_request
		
		return JSONResponse(content="Success", status_code=status.HTTP_201_CREATED)
	except:
		return bad_request

@app.put("/employees/{employee_id}")
async def put_employee(employee_id: int, req: Request):
	"""Updates an employee.

	You can assume the body of the PUT request is a JSON object that represents an employee.
	You can assume the request body contains only attributes found in the employee table and does not
	attempt to update `employee_id`.

	For instance,
		PUT http://0.0.0.0:8002/employees/1
		{
			"first_name": "Donald"
		}
	should update the employee with employee ID 1's first name to Donald.

	If the employee does not exist, the HTTP status should be set to 404 Not Found.
	If the email is set to null in the request body, the HTTP status should be set to 400 Bad Request.
	If the email already exists in the employee table, the HTTP status should be set to 400 Bad Request.
	If the employee type is not valid, the HTTP status should be set to 400 Bad Request.

	:param employee_id: The ID of the employee to be updated
	:param req: The request, which contains an employee JSON in its body
	:returns: If the request is valid, the HTTP status should be set to 200 OK.
				If the request is not valid, the HTTP status should be set to the appropriate error code.
	"""

	# Use `await req.json()` to access the request body
	employee_data = await req.json()
	bad_request = JSONResponse(content="bad request",status_code=status.HTTP_400_BAD_REQUEST)

	filters = {'employee_id': employee_id}
	employee = db.select('employee', None, filters)
	if not employee: 
		return JSONResponse(content="",status_code=status.HTTP_404_NOT_FOUND)
	
	if 'email' in employee_data:
		if employee_data['email'] == None:
			return bad_request 

		existing_employees = db.select('employee', None, {'email': employee_data['email']})
		if existing_employees:
			return bad_request
	

	
	
	try:
		employee_type = employee_data['employee_type']
		if employee_type not in ['Professor', 'Lecturer', 'Staff']:
			return bad_request
	except:
		bad_request

	
	try:
		n_rows = db.update('employee', employee_data, filters)
		if n_rows == 0:
			return bad_request
		
		return JSONResponse(content="Success", status_code=status.HTTP_200_OK)
	except:
		return bad_request

@app.delete("/employees/{employee_id}")
async def delete_employee(employee_id: int):
	"""Deletes an employee.

	For instance,
		DELETE http://0.0.0.0:8002/employees/1
	should delete the employee with employee ID 1.

	If the employee does not exist, the HTTP status should be set to 404 Not Found.

	:param employee_id: The ID of the employee to be deleted
	:returns: If the request is valid, the HTTP status should be set to 200 OK.
				If the request is not valid, the HTTP status should be set to 404 Not Found.
	"""
	filters = {'employee_id': employee_id}
	employee = db.select('employee', None, filters)
	if not employee: 
		return JSONResponse(content="",status_code=status.HTTP_404_NOT_FOUND)
	
	bad_request = JSONResponse(content="bad request",status_code=status.HTTP_400_BAD_REQUEST)

	try:
		n_rows = db.delete('employee', filters)
		if n_rows == 0:
			return bad_request
		
		return JSONResponse(content="Success", status_code=status.HTTP_200_OK)
	except:
		return bad_request

if __name__ == "__main__":
	uvicorn.run(app, host="0.0.0.0", port=8002)
