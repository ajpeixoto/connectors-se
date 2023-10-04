def slackChannel = 'components-ci'

def nexusCredentials = usernamePassword(
	credentialsId: 'nexus-artifact-zl-credentials',
    usernameVariable: 'NEXUS_USER',
    passwordVariable: 'NEXUS_PASSWORD')
def gitCredentials = usernamePassword(
	credentialsId: 'github-credentials',
    usernameVariable: 'GITHUB_LOGIN',
    passwordVariable: 'GITHUB_TOKEN')
def dockerCredentials = usernamePassword(
	credentialsId: 'artifactory-datapwn-credentials',
    passwordVariable: 'ARTIFACTORY_PASSWORD',
    usernameVariable: 'ARTIFACTORY_LOGIN')

def PRODUCTION_DEPLOYMENT_REPOSITORY = "TalendOpenSourceSnapshot"

def branchName = env.BRANCH_NAME
if (BRANCH_NAME.startsWith("PR-")) {
    branchName = env.CHANGE_BRANCH
}

def escapedBranch = branchName.toLowerCase().replaceAll("/", "_")
def deploymentSuffix = (env.BRANCH_NAME == "master" || env.BRANCH_NAME.startsWith("maintenance/")) ? "${PRODUCTION_DEPLOYMENT_REPOSITORY}" : ("dev_branch_snapshots/branch_${escapedBranch}")

def m2 = "/tmp/jenkins/tdi/m2/${deploymentSuffix}"
def talendOssRepositoryArg = (env.BRANCH_NAME == "master" || env.BRANCH_NAME.startsWith("maintenance/")) ? "" : ("-Dtalend_oss_snapshots=https://nexus-smart-branch.datapwn.com/nexus/content/repositories/${deploymentSuffix}")

def calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

def podLabel = "connectors-se-${UUID.randomUUID().toString()}".take(53)
final String repository = 'connectors-se'

def EXTRA_BUILD_PARAMS = ""

// Pod definition
final String podDefinition = """\
    apiVersion: v1
    kind: Pod
    spec:
      imagePullSecrets:
        - name: talend-registry
      containers:
        - name: main
          image: '${env.TSBI_IMAGE}'
          command: [ cat ]
          tty: true
          volumeMounts: [
            { name: efs-jenkins-connectors-se-m2, mountPath: /root/.m2/repository }
          ]
          resources: { requests: { memory: 3G, cpu: '2' }, limits: { memory: 8G, cpu: '2' } }
          env: 
            - name: DOCKER_HOST
              value: tcp://localhost:2375
        - name: docker-daemon
          image: artifactory.datapwn.com/docker-io-remote/docker:19.03.1-dind
          env:
            - name: DOCKER_TLS_CERTDIR
              value: ""
          securityContext:
            privileged: true
      volumes:
        - name: efs-jenkins-connectors-se-m2
          persistentVolumeClaim: 
            claimName: efs-jenkins-connectors-se-m2
""".stripIndent()

pipeline {
    agent {
        kubernetes {
            label podLabel
            yaml podDefinition
        }
    }

    environment {
        MAVEN_OPTS = "-Dmaven.artifact.threads=128 -Dorg.slf4j.simpleLogger.showThreadName=true -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss -Dtalend.maven.decrypter.m2.location=${WORKSPACE}/.jenkins/"
        TALEND_REGISTRY = 'registry.datapwn.com'

        VERACODE_APP_NAME = 'Talend Component Kit'
        VERACODE_SANDBOX = 'connectors-se'
        APP_ID = '579232'
        ARTIFACTORY_REGISTRY = "artifactory.datapwn.com"
        TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX="artifactory.datapwn.com/docker-io-remote/"
    }

    options {
        buildDiscarder(logRotator(artifactNumToKeepStr: '5', numToKeepStr: (env.BRANCH_NAME == 'master' || env.BRANCH_NAME.startsWith('maintenance/')) ? '10' : '2'))
        timeout(time: 120, unit: 'MINUTES')
        skipStagesAfterUnstable()
    }

    triggers {
        cron(env.BRANCH_NAME == "master" ? "@daily" : "")
    }

    parameters {
        choice(name: 'Action', 
               choices: [ 'STANDARD', 'PUSH_TO_XTM', 'DEPLOY_FROM_XTM', 'RELEASE' ],
               description: 'Kind of running : \nSTANDARD (default), normal building\n PUSH_TO_XTM : Export the project i18n resources to Xtm to be translated. This action can be performed from master or maintenance branches only. \nDEPLOY_FROM_XTM: Download and deploy i18n resources from Xtm to nexus for this branch.\nRELEASE : build release')
        string(name: 'EXTRA_BUILD_PARAMS', defaultValue: "", description: 'Add some extra parameters to maven commands. Applies to all maven calls.')
        string(name: 'POST_LOGIN_SCRIPT', defaultValue: "", description: 'Execute a shell command after login. Useful for maintenance.')
        booleanParam(
                name: 'DRAFT_CHANGELOG',
                defaultValue: true,
                description: '''
            Create a draft release changelog. User will need to approve it on github.
            Only used on release action''')
    }

    stages {
        stage('Docker login') {
            steps {
                container('main') {
                    withCredentials([dockerCredentials]) {
                        sh '''#!/bin/bash
                        env|sort
                        docker version
                        echo $ARTIFACTORY_PASSWORD | docker login $ARTIFACTORY_REGISTRY -u $ARTIFACTORY_LOGIN --password-stdin
                        '''
                    }
                }
                script {
                    try {
                        EXTRA_BUILD_PARAMS = params.EXTRA_BUILD_PARAMS
                    } catch (error) {
                        EXTRA_BUILD_PARAMS = ""
                    }
                }
            }
        }
        stage('Git login') {
            steps {
                container('main') {
                    script {
                        echo 'Git login'
                        withCredentials([gitCredentials]) {
                            sh """
                            bash .jenkins/git-login.sh \
                                "\${GITHUB_LOGIN}" \
                                "\${GITHUB_TOKEN}"
                        """
                        }
                    }
                }
            }
        }
        stage('Post login') {
            steps {
                container('main') {
                    withCredentials([nexusCredentials, gitCredentials, dockerCredentials]) {
                        script {
                            if (params.POST_LOGIN_SCRIPT?.trim()) {
                                try {
                                    sh "${params.POST_LOGIN_SCRIPT}"
                                } catch (error) {
                                    //
                                }
                            }
                        }
                    }
                }
            }
        }
        stage('Run maven') {
            when {
                expression { params.Action == 'STANDARD' }
            }
            steps {
                container('main') {
                    // for next concurrent builds
                    sh 'for i in ci_documentation ci_nexus ci_site; do rm -Rf $i; rsync -av . $i; done'
                    // real task
                    withCredentials([nexusCredentials]) {
                        script {
                            sh "mvn ${EXTRA_BUILD_PARAMS} -B -s .jenkins/settings.xml clean install -PITs -Dtalend.maven.decrypter.m2.location=${env.WORKSPACE}/.jenkins/ -e ${talendOssRepositoryArg}"
                        }
                    }
                }
            }
            post {
                always {
                    junit testResults: '**/target/surefire-reports/*.xml', allowEmptyResults: true
                    publishHTML(target: [
                            allowMissing: false, alwaysLinkToLastBuild: false, keepAll: true,
                            reportDir   : 'target/talend-component-kit', reportFiles: 'icon-report.html', reportName: "Icon Report"
                    ])
                    publishHTML(target: [
                            allowMissing: false, alwaysLinkToLastBuild: false, keepAll: true,
                            reportDir   : 'target/talend-component-kit', reportFiles: 'repository-dependency-report.html', reportName: "Dependencies Report"
                    ])
                }
            }
        }
        stage('Post Build Steps') {
            when {
                expression { params.Action == 'STANDARD' }
            }
            parallel {
                stage('Documentation') {
                    when {
                        anyOf {
                            branch 'master'
                            expression { env.BRANCH_NAME.startsWith('maintenance/') }
                        }
                    }
                    steps {
                        container('main') {
                            withCredentials([dockerCredentials]) {
                                sh """
			                     |cd ci_documentation
			                     |mvn ${EXTRA_BUILD_PARAMS} -B -s .jenkins/settings.xml clean install -DskipTests
			                     |chmod +x .jenkins/generate-doc.sh && .jenkins/generate-doc.sh
			                     |""".stripMargin()
                            }
                        }
                    }
                    post {
                        always {
                            publishHTML(target: [
                                    allowMissing: true, alwaysLinkToLastBuild: false, keepAll: true,
                                    reportDir   : 'ci_documentation/target/talend-component-kit_documentation/', reportFiles: 'index.html', reportName: "Component Documentation"
                            ])
                        }
                    }
                }
                stage('Site') {
                    when {
                        anyOf {
                            branch 'master'
                            expression { env.BRANCH_NAME.startsWith('maintenance/') }
                        }
                    }
                    steps {
                        container('main') {
                            sh 'cd ci_site && mvn ${EXTRA_BUILD_PARAMS} -B -s .jenkins/settings.xml clean site site:stage -Dmaven.test.failure.ignore=true'
                        }
                    }
                    post {
                        always {
                            publishHTML(target: [
                                    allowMissing: true, alwaysLinkToLastBuild: false, keepAll: true,
                                    reportDir   : 'ci_site/target/staging', reportFiles: 'index.html', reportName: "Maven Site"
                            ])
                        }
                    }
                }
                stage('Nexus') {
                    when {
                        anyOf {
                            branch 'master'
                            expression { env.BRANCH_NAME.startsWith('maintenance/') }
                        }
                    }
                    steps {
                        container('main') {
                            withCredentials([nexusCredentials]) {
                                sh "cd ci_nexus && mvn ${EXTRA_BUILD_PARAMS} -B -s .jenkins/settings.xml clean deploy -e -Pdocker -DskipTests ${talendOssRepositoryArg}"
                            }
                        }
                    }
                }
            }
        }
        stage('Push to Xtm') {
            when {
                anyOf {
                    expression { params.Action == 'PUSH_TO_XTM' }
                    /*allOf {
                        triggeredBy 'TimerTrigger'
                        expression { calendar.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY }
                    }*/
                }
                anyOf {
                    branch 'master'
                    expression { env.BRANCH_NAME.startsWith('maintenance/') }
                }
            }
            steps {
                container('main') {
                    withCredentials([nexusCredentials, string(credentialsId: 'xtm-token', variable: 'XTM_TOKEN')]) {
                        script {
                            sh "mvn ${EXTRA_BUILD_PARAMS} -e -B clean && mvn ${EXTRA_BUILD_PARAMS} -e -B -s .jenkins/settings.xml clean package -pl . -Pi18n-export"
                        }
                    }
                }
            }
        }
        stage('Deploy from Xtm') {
            when {
                expression { params.Action == 'DEPLOY_FROM_XTM' }
                anyOf {
                    branch 'master'
                    expression { env.BRANCH_NAME.startsWith('maintenance/') }
                }
            }
            steps {
                container('main') {
                    withCredentials([nexusCredentials, string(credentialsId: 'xtm-token', variable: 'XTM_TOKEN'), gitCredentials]) {
                        script {
                            sh "sh .jenkins/xtm-deploy.sh"
                        }
                    }
                }
            }
        }
        stage('Release') {
            when {
                expression { params.Action == 'RELEASE' }
                anyOf {
                    branch 'master'
                    expression { BRANCH_NAME.startsWith('maintenance/') }
                }
            }
            steps {
                withCredentials([gitCredentials, nexusCredentials]) {
                    container('main') {
                        sh "sh .jenkins/release.sh"
                    }
                }
            }
        }

        stage('Release changelog') {
            when {
                expression { params.Action == 'RELEASE' }
            }
            steps {
                withCredentials([gitCredentials]) {
                    container('main') {
                        // Do not failed the build in case of changelog issue.
                        catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                            script {
                                final def pom = readMavenPom file: 'pom.xml'
                                String pomVersion = pom.version
                                String releaseVersion = evaluatePreviousVersion(pomVersion.split('-')[0])
                                String previousVersion = evaluatePreviousVersion(releaseVersion)
                                sh """
                                    bash .jenkins/changelog.sh \
                                        '${repository}' \
                                        '${previousVersion}' \
                                        '${releaseVersion}' \
                                        '${params.DRAFT_CHANGELOG}' \
                                        "\${BRANCH_NAME}" \
                                        "\${GITHUB_LOGIN}" \
                                        "\${GITHUB_TOKEN}"
                                """
                            }
                        }
                    }
                }
            }
        }
    }
    post {
        success {
            slackSend(color: '#00FF00', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", channel: "${slackChannel}")
        }
        failure {
            slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})", channel: "${slackChannel}")
        }
    }
}

/**
 * Evaluate previous SemVer version
 * @param version current version
 * @return previous version
 */
private static String evaluatePreviousVersion(String version) {
    def components = version.split('\\.')

    int major = components[0] as int
    int minor = components[1] as int
    int patch = components[2] as int

    if (patch > 0) {
        patch--
    } else {
        patch = 0
        if (minor > 0) {
            minor--
        } else {
            minor = 0
            if (major > 0) {
                major--
            } else {
                // Invalid state: Cannot calculate previous version if major version is already 0 or less
                throw new IllegalArgumentException("Invalid version: $version")
            }
        }
    }

    return "${major}.${minor}.${patch}"
}