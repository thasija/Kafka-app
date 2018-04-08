pipeline {
	agent {
		label 'master'
		jdk 'jdk8'
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
