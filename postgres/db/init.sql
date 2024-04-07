-- Tabela studentów
CREATE TABLE students (
    student_id UUID PRIMARY KEY,
    name VARCHAR(50),
    surname VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(15),
    program_name VARCHAR(50),
    semester INTEGER,
    study_mode VARCHAR(20)
);

-- Tabela kursów
CREATE TABLE courses (
    course_id UUID PRIMARY KEY,
    course_name VARCHAR(50),
    instructor VARCHAR(50),
    room VARCHAR(50),
    credits INTEGER
);

-- Tabela zapisów
CREATE TABLE enrollments (
    enrollment_id UUID PRIMARY KEY,
    student_id UUID,
    course_id UUID,
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
);
