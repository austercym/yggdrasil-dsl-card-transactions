@Library('jenkins-shared-libs@feature/lib_refactor')_

pipeline {

    agent { label 'docker'}

    environment {
        ARTIFACTORY_SERVER_REF = 'artifactory'

        artifactVersion = "${new Date().format('yy.MM.dd')}"
        pomPath = 'pom.xml'

        snapshotRepository = 'libs-snapshot-local'
        releaseRepository = 'libs-release-local'
        snapshotDependenciesRepository = 'libs-snapshot'
        releaseDependeciesRepository = 'libs-release'


    }

    stages {

        stage('Pipeline setup') {
            parallel {
                stage('Triggers setup') {
                    agent{
                        docker {
                            image 'maven:3-alpine'
                            args '-v $HOME/.m2:/root/.m2 --network user-default'
                            reuseNode true
                            label 'docker'
                        }
                    }
                    steps {
                        script {
                            withCredentials([string(credentialsId: 'github-orwell-cicd-webhook-token', variable: 'githubWebhookGenericToken')]) {
                                properties([
                                        pipelineTriggers([
                                                [
                                                        $class                   : 'GenericTrigger',
                                                        causeString              : 'Push made',
                                                        token                    : githubWebhookGenericToken,
                                                        genericHeaderVariables   : [
                                                                [key: 'X-GitHub-Event', regexpFilter: '']
                                                        ],
                                                        genericVariables         : [
                                                                [key: 'project', value: '$.repository.name'],
                                                                [key: 'branch', value: '$.ref']
                                                        ],
                                                        regexpFilterExpression   : (env.JOB_NAME.tokenize('/'))[0] + ',push',
                                                        regexpFilterText         : '$project,$x_github_event',
                                                        printContributedVariables: true,
                                                        printPostContent         : true
                                                ]
                                        ])
                                ])
                            }
                        }
                    }
                }

                stage('Artifactory setup') {
                    agent{
                        docker {
                            image 'maven:3-alpine'
                            args '-v $HOME/.m2:/root/.m2 --network  user-default'
                            reuseNode true
                            label 'docker'
                        }
                    }
                    steps {
                        script {
                            def MAVEN_HOME = sh(script: 'echo $MAVEN_HOME', returnStdout: true).trim()
                            // Obtain an Artifactory server instance, defined in Jenkins --> Manage:
                            server = Artifactory.server ARTIFACTORY_SERVER_REF

                            def  descriptor = Artifactory.mavenDescriptor()
                            descriptor.pomFile = pomPath
                            if (!descriptor.version.endsWith('-SNAPSHOT'))
                                artifactVersion = artifactVersion + '-SNAPSHOT'
                            descriptor.version = artifactVersion
                            descriptor.transform()

                            rtMaven = Artifactory.newMavenBuild()
                            env.MAVEN_HOME = MAVEN_HOME
                            rtMaven.deployer releaseRepo: releaseRepository, snapshotRepo: snapshotRepository, server: server
                            rtMaven.resolver releaseRepo: releaseDependeciesRepository, snapshotRepo: snapshotDependenciesRepository, server: server
                            rtMaven.opts = '-DprofileIdEnabled=true'
                            rtMaven.deployer.deployArtifacts = false // Disable artifacts deployment during Maven run

                            buildInfo = Artifactory.newBuildInfo()
                        }
                    }
                }
            }
        }

        stage('Unit test') {
            agent{
                docker {
                    image 'maven:3-alpine'
                    args '-v $HOME/.m2:/root/.m2 --network user-default'
                    reuseNode true
                    label 'docker'
                }
            }
            steps {

                    script {
                        rtMaven.run pom: pomPath, goals: '-U clean compile test -Pdeploy'
                    }
                
            }

            post {
                  always {
                      junit 'target/surefire-reports/*.xml'
                  }
            }
        }

	stage('SonarAnalysis') {
		agent {
			docker {
				image 'docker.registry.service.cicd.consul/sonarqube-scanner:latest'
				args '-u root:root'
				reuseNode true
			}
		}
		steps {
			withSonarQubeEnv('SonarQube') {
				sh "sudo sonar-scanner -Dsonar.host.url=http://11.0.200.26:9000 -Dsonar.projectKey=${env.JOB_NAME.replace('/','_')} -Dsonar.sources=./src/main -Dsonar.java.binaries=."
			}
		}
		post {
                    always {
                        echo 'inside post always sonar stage'
			sh 'rm -rf .scannerwork'
                    }
                }
	}

        stage('Build') {
            agent{
                docker {
                    image 'maven:3-alpine'
                    args '-v $HOME/.m2:/root/.m2 --network user-default'
                    reuseNode true
                    label 'docker'
                }
            }
            steps {
                script {
                    rtMaven.run pom: pomPath, goals: '-U clean package -Pdeploy', buildInfo: buildInfo
                }
            }

            post {
                always {
                    archiveArtifacts artifacts: '**/target/*.jar'
                }
            }
        }

        stage('Publish') {
            steps {
                script {
                    server.publishBuildInfo buildInfo
                    rtMaven.deployer.deployArtifacts buildInfo
                }
            }
        }


        stage('Deploy') {
            steps {
                script {
                    pom = readMavenPom file: pomPath
                    pom = readMavenPom file: pomPath
                    kUser =  'svc_cards'
                    hostsDeploy = 'sid-hdf-g4-1.node.sid.consul'
                    nimbusHost = 'sid-hdf-g1-1.node.sid.consul'
                    zkHost = 'sid-hdf-g1-0.node.sid.consul:2181,sid-hdf-g1-1.node.sid.consul:2181,sid-hdf-g1-2.node.sid.consul:2181'
                    mainClass = 'com.orwellg.yggdrasil-card-account-balance-update'
                    groupId = 'com.orwellg'
                    stormDeploy  pom.artifactId  , pom.version , groupId, kUser  ,  hostsDeploy , nimbusHost ,zkHost, mainClass
                }
            }


        }
        
        stage('Remove_Workspace') {
            steps {
                deleteDir()
            }
        }
    }
}

