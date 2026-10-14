
pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo 'Repository checked out by Jenkins'
            }
        }

        stage('Build') {
            steps {
                sh '''
                    if [ -f mvnw ]; then
                        chmod +x mvnw
                        ./mvnw clean package
                    elif [ -f pom.xml ]; then
                        mvn clean package
                    else
                        echo "No Maven project found"
                        exit 1
                    fi
                '''
            }
        }
    }
}
