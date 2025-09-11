#!/bin/bash

# Setup script for the Forward Deployed Engineering Technical Test
# This script initializes the environment

echo "🚀 Setting up Foward Deployed Engineering Technical Test Environment"
echo "============================================================"

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3 and try again."
    exit 1
fi

echo "✅ Python 3 is available"

# Install Python dependencies for local testing
echo "📦 Installing Python dependencies for testing..."
if [ -f "api/requirements.txt" ]; then
    pip3 install -r api/requirements.txt
    echo "✅ Python dependencies installed"
else
    echo "⚠️  Requirements file not found, skipping dependency installation"
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

echo "✅ Docker is running"

# Start the environment
echo "🐳 Starting Docker environment..."
docker-compose up --build -d

# Wait a moment for the service to start
echo "⏳ Waiting for API to start..."
sleep 5

# Test if the API is running
if curl -s http://localhost:5000/ > /dev/null; then
    echo "✅ API is running at http://localhost:5000"
    echo "📖 API documentation available at http://localhost:5000/docs"
else
    echo "⚠️  API might still be starting. Check docker-compose logs if needed."
fi

echo ""
echo "🎯 Environment is ready!"
echo "👉 Candidates should read TASK.md for instructions"
echo ""
echo "To stop the environment, run: docker-compose down"
