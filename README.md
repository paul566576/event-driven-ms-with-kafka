# Event-Driven Microservices Example

This project demonstrates an event-driven architecture with microservices that communicate through Kafka. 
It includes several microservices running in Docker containers and showcases how Spring Boot can be used to develop services that interact via Kafka.

## Project Components

The project includes the following components:

- **Products Microservice** — a service that manages products. It publishes events about new products being created to Kafka.
- **Email Notification Microservice** — a service that processes product creation events and sends email notifications.
- **Mock Service** — a helper service that simulates external interactions.
- **Kafka** — a distributed messaging platform used for event communication between the services.

## Technologies

- **Spring Boot** — for building the microservices.
- **Apache Kafka** — for message-driven communication between the services.
- **Docker** — for containerizing all the services.
- **Docker Compose** — for orchestrating the services.

## Prerequisites

- **Docker** and **Docker Compose** installed.

## Setup and Run

### 1. Clone the repository

Clone the project from GitHub:

```bash
git clone https://github.com/your-repository.git
cd your-repository
