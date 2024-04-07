var database = db.getSiblingDB('nazwa_bazy_danych');

// Tworzenie kolekcji 'students'
database.createCollection('students');

// Tworzenie kolekcji 'courses'
database.createCollection('courses');

// Tworzenie użytkownika z rolą 'readWrite' w bazie danych 'nazwa_bazy_danych'
database.createUser({
    user: "mongo",
    pwd: "mongo",
    roles: [{ role: "readWrite", db: "mongo" }]
});
