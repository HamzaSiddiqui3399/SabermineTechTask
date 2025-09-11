@echo off

echo 🚀 Setting up Forward Deployed Engineering Technical Test Environment
echo ============================================================

REM Check if Python 3 is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python 3 is not installed. Please install Python 3 and try again.
    exit /b 1
)

echo ✅ Python 3 is available

REM Install Python dependencies for local testing
echo 📦 Installing Python dependencies for testing...
if exist "api\requirements.txt" (
    pip install -r api\requirements.txt
    echo ✅ Python dependencies installed
) else (
    echo ⚠️  Requirements file not found, skipping dependency installation
)

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not running. Please start Docker and try again.
    exit /b 1
)

REM Check if Docker Compose is available
docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker Compose is not installed. Please install Docker Compose and try again.
    exit /b 1
)

echo ✅ Docker is running

REM Start the environment
echo 🐳 Starting Docker environment...
docker-compose up --build -d

REM Wait a moment for the service to start
echo ⏳ Waiting for API to start...
timeout /t 5 /nobreak >nul

REM Test if the API is running
curl -s http://localhost:5000/ >nul 2>&1
if %errorlevel% equ 0 (
    echo ✅ API is running at http://localhost:5000
    echo 📖 API documentation available at http://localhost:5000/docs
) else (
    echo ⚠️  API might still be starting. Check docker-compose logs if needed.
)

echo.
echo 🎯 Environment is ready!
echo 👉 Candidates should read TASK.md for instructions
echo.
echo To stop the environment, run: docker-compose down