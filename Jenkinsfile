pipeline {
    agent {
        label 'built-in'
    }

    environment {
        DB_NODES = '3'
        TEST_EXIT_CODE = '0'
        PATH = "/usr/local/bin:${env.PATH}"
    }

    stages {
        stage('Checkout SCM') {
            steps {
                checkout scm
            }
        }

        stage('Run') {
            steps {
                script {
                    sh 'chmod +x docker/gorgon_couchbase/up.sh'
                    env.TEST_EXIT_CODE = sh(
                        script: 'cd docker/gorgon_couchbase && DB_NODES=${DB_NODES} ./up.sh',
                        returnStatus: true
                    ).toString()
                }
            }
        }

        stage('Copy Artifacts from Container') {
            steps {
                sh '''
                    if [ "${TEST_EXIT_CODE}" = "1" ]; then
                        cd docker/gorgon_couchbase && docker compose -f compose.yaml up -d
                        cd ../..

                        docker cp gorgon-control:/files.tgz .
                        docker cp gorgon-control:/root/store ./store

                        mkdir -p cbcollects_and_captures
                        for i in $(seq 0 $((DB_NODES-1))); do
                            docker cp gorgon-n$i:/root/cbcollects_and_captures/. ./cbcollects_and_captures/
                        done
                    fi
                '''
            }
        }

        stage('Archive Artifacts') {
            steps {
                script {
                    if (env.TEST_EXIT_CODE == '1') {
                        archiveArtifacts artifacts: 'files.tgz, store/**, cbcollects_and_captures/**',
                                         fingerprint: true
                    }
                }
            }
        }
    }

    post {
        always {
            sh 'docker compose -f docker/gorgon_couchbase/compose.yaml down'
        }
    }
}