pipeline {
  agent any
  stages {
    stage('package') {
      agent any
      steps {
        sh 'cd complete'
        sh 'mvn package'
      }
    }

    stage('run') {
      agent any
      steps {
        sh 'java -jar complete/target/gs-rest-service-0.1.0.jar'
      }
    }

  }
}