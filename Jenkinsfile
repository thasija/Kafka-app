pipeline {
	agent {
		label 'master'
	  }
	  tools {
	     maven 'Maven 3.5.3'
	  }
    stages {
        stage('Build') { 
            steps {
                sh 'mvn -B -DskipTests clean package' 
            }
        }
    }
}
