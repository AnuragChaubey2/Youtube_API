# Use an official Golang runtime as the base image
FROM golang:latest

# Set the working directory in the container
WORKDIR /app

# Copy the source code into the container
COPY . .

# Build the Go app
RUN go build -o main .

# Expose port 8080 to the host
EXPOSE 8080

# Set environment variables
ENV DB_USER=anurag \
    DB_PASSWORD=/run/secrets/db_password \
    DB_NAME=youtube \
    API_KEY=/run/secrets/api_key

# Run the binary
CMD ["./main"]
