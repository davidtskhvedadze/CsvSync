<h1 align="center">
    CsvToDatabaseImporter
</h1>
<div align="center">
CsvToDatabaseImporter is a Groovy-based application that reads data from a CSV file and imports it into a MySQL database using Apache Camel.
</div>

## Tech Stack
<div align="center">
    <img src="https://skillicons.dev/icons?i=java,mysql,gradle" alt="Tech Stack">
</div>
This stack represents the core technologies behind CsvToDatabaseImporter, including its backend service built with Groovy and MySQL for managing data importation, and Gradle for project management and build automation.

## Features

- Reads data from a CSV file and imports it into a MySQL database.
- Utilizes Apache Camel for routing and mediation rules.
- Secure database connection using environment variables for credentials.
- Logs each step of the process for easy debugging and monitoring.
- Supports dynamic CSV file input through command-line arguments.

## Prerequisites

- Java 17
- Groovy
- MySQL
- Gradle

## Installation

1. Clone the repository.
2. Access the directory.
3. Create a `.env` file in the root directory with the following content:
    ```properties
    DB_URL=localhost:3306/your_dbname
    DB_USER=user
    DB_PASSWORD=password
    ```
4. Install the dependencies: `./gradlew build`

## Usage

To run the application, use the following command:
`./gradlew run --args=<csv-file-name>`
Replace `<csv-file-name>` with the name of your CSV file.

## CSV File Format

The CSV file should have the following format:
```plaintext
header1,header2,header3, ...
data1,data2,data3, ...
data1,data2,data3, ...
.
.
.
```

Place the file in the `csv` directory within the `src` folder.

## Notes
- Ensure that your MySQL database is running and accessible.
- The database connection details should be correctly set in the .env file.
- The application logs each step of the process, which can be useful for debugging and monitoring.