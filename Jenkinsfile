//----------------- Credentials
final def nexusCredentials = usernamePassword(
        credentialsId: 'nexus-artifact-zl-credentials',
        usernameVariable: 'NEXUS_USER',
        passwordVariable: 'NEXUS_PASSWORD')
final def gitCredentials = usernamePassword(
        credentialsId: 'github-credentials',
        usernameVariable: 'GITHUB_LOGIN',
        passwordVariable: 'GITHUB_TOKEN')
final def artifactoryCredentials = usernamePassword(
        credentialsId: 'artifactory-datapwn-credentials',
        passwordVariable: 'ARTIFACTORY_PASSWORD',
        usernameVariable: 'ARTIFACTORY_LOGIN')
def sonarCredentials = usernamePassword(
        credentialsId: 'sonar-credentials',
        passwordVariable: 'SONAR_PASSWORD',
        usernameVariable: 'SONAR_LOGIN')


// Job config
final String slackChannel = 'components-ci'
final String PRODUCTION_DEPLOYMENT_REPOSITORY = "TalendOpenSourceSnapshot"
final String branchName = BRANCH_NAME.startsWith("PR-")
        ? env.CHANGE_BRANCH
        : env.BRANCH_NAME
String releaseVersion = ''
String extraBuildParams = ''
Boolean fail_at_end = false
final String escapedBranch = branchName.toLowerCase().replaceAll("/", "_")
final boolean isOnMasterOrMaintenanceBranch = env.BRANCH_NAME == "master" || env.BRANCH_NAME.startsWith("maintenance/")
final GString devNexusRepository = isOnMasterOrMaintenanceBranch
        ? "${PRODUCTION_DEPLOYMENT_REPOSITORY}"
        : "dev_branch_snapshots/branch_${escapedBranch}"
final Boolean hasPostLoginScript = params.POST_LOGIN_SCRIPT != ""
final Boolean hasExtraBuildArgs = params.EXTRA_BUILD_PARAMS != ""

// Pod config
final String podLabel = "connectors-se-${UUID.randomUUID().toString()}".take(53)
final String tsbiImage = 'jdk11-svc-springboot-builder'
final String tsbiVersion = '2.9.18-2.4-20220104141654'

// Files and folder definition
final String _COVERAGE_REPORT_PATH = '**/jacoco-aggregate/jacoco.xml'

// Artifacts paths
final String _ARTIFACT_COVERAGE = '**/target/site/**/*.*'

// Pod definition
final String podDefinition = """\
    apiVersion: v1
    kind: Pod
    spec:
      imagePullSecrets:
        - name: talend-registry
      containers:
        - name: '${tsbiImage}'
          image: 'artifactory.datapwn.com/tlnd-docker-dev/talend/common/tsbi/${tsbiImage}:${tsbiVersion}'
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
        MAVEN_SETTINGS = "${WORKSPACE}/.jenkins/settings.xml"
        DECRYPTER_ARG = "-Dtalend.maven.decrypter.m2.location=${env.WORKSPACE}/.jenkins/"
        MAVEN_OPTS = [
                "-Dmaven.artifact.threads=128",
                "-Dorg.slf4j.simpleLogger.showDateTime=true",
                "-Dorg.slf4j.simpleLogger.showThreadName=true",
                "-Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss"
        ].join(' ')
        VERACODE_APP_NAME = 'Talend Component Kit'
        VERACODE_SANDBOX = 'connectors-se'

        APP_ID = '579232'
        TALEND_REGISTRY = "artifactory.datapwn.com"

        TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX = "artifactory.datapwn.com/docker-io-remote/"
    }

    options {
        buildDiscarder(
                logRotator(
                        artifactNumToKeepStr: '5',
                        numToKeepStr: isOnMasterOrMaintenanceBranch ? '10' : '2'
                )
        )
        timeout(time: 60, unit: 'MINUTES')
        skipStagesAfterUnstable()
    }

    triggers {
        cron(env.BRANCH_NAME == "master" ? "@daily" : "")
    }

    parameters {
        choice(
          name: 'Action',
          choices: ['STANDARD', 'RELEASE', 'DEPLOY'],
          description: '''
            Kind of run:
            STANDARD : (default) classical CI
            RELEASE : Build release, deploy to the Nexus for master/maintenance branches
            DEPLOY : Build snapshot, deploy it to the Nexus for any branch''')
        string(
          name: 'NEXUS_QUALIFIER',
          defaultValue: 'DEFAULT',
          description: '''
            For branches, used qualifier to store dependencies in the nexus.
            DEFAULT is {x.y.z}-SNAPSHOT-{jira-id} with:
              - {x.y.z} the version read form pom.xml
              - {jira-id} taken from branch name that has to be in the format: user/TDI-XXXXX_Custom_description
              example: 1.38.0-SNAPSHOT-TDI-48419''')
        choice(
          name: 'FAIL_AT_END',
          choices: ['DEFAULT', 'YES', 'NO'],
          description: '''
            Choose to add "--fail-at-end" in the maven build
              - DEFAULT : "--fail-at-end" activated for master and maintenance, not for others branches
              - YES : Force the use of "--fail-at-end" 
              - NO : Force to not use "--fail-at-end"''')
        booleanParam(
          name: 'SONAR_ANALYSIS',
          defaultValue: true,
          description: 'Execute Sonar analysis (only for STANDARD action).')
        string(
          name: 'EXTRA_BUILD_PARAMS',
          defaultValue: "",
          description: 'Add some extra parameters to maven commands. Applies to all maven calls.')
        string(name: 'POST_LOGIN_SCRIPT',
          defaultValue: "",
          description: 'Execute a shell command after login. Useful for maintenance.')
        string(name: 'DEV_NEXUS_REPOSITORY',
          defaultValue: devNexusRepository,
          description: 'The Nexus repositories where maven snapshots are deployed.')
        booleanParam(name: 'DEBUG_BEFORE_EXITING',
          defaultValue: false,
          description: 'Add an extra step to the pipeline allowing to keep the pod alive for debug purposes')
    }

    stages {
        stage('Validate parameters') {
            steps {
                script {
                    final def pom = readMavenPom file: 'pom.xml'
                    final String pomVersion = pom.version

                    if (params.Action == 'RELEASE' && !pomVersion.endsWith('-SNAPSHOT')) {
                        error('Cannot release from a non SNAPSHOT, exiting.')
                    }

                    if (params.Action == 'RELEASE' && !((String) env.BRANCH_NAME).startsWith('maintenance/')) {
                        error('Can only release from a maintenance branch, exiting.')
                    }


                    echo 'Manage the DEV_NEXUS_REPOSITORY storage'
                    if (isOnMasterOrMaintenanceBranch) {
                        echo 'No DEV_NEXUS_REPOSITORY user for a Master or Maintenance branch'
                    }
                    else {
                        // Validate the branch name
                        String brancherRegex = /^(?<user>.*)\/(?<ticket>[A-Z]{2,4}-\d{1,6})_(?<description>.*)/
                        Matcher branchMatcher = "$env.BRANCH_NAME" =~ brancherRegex
                        assert branchMatcher.matches()

                        def branch_user = branchMatcher.group("user")
                        def branch_ticket =  branchMatcher.group("ticket")
                        def branch_description =  branchMatcher.group("description")

                        echo """
                          Configure the DEV_NEXUS_REPOSITORY for the curent branche: $env.BRANCH_NAME
                          with User = $branch_user, Ticket = $branch_ticket, Description = $branch_description"""
                        echo

                    }

                    echo 'Processing parameters'
                    final ArrayList buildParamsAsArray = ['--settings', env.MAVEN_SETTINGS, env.DECRYPTER_ARG]
                    if (!isOnMasterOrMaintenanceBranch) {
                        // Properties documented in the pom.
                        buildParamsAsArray.addAll([
                                '--define', "nexus_snapshots_repository=${params.DEV_NEXUS_REPOSITORY}",
                                '--define', 'nexus_snapshots_pull_base_url=https://nexus-smart-branch.datapwn.com/nexus/content/repositories'
                        ])
                    }

                    // Manage the failed at-end-option
                    if( (isOnMasterOrMaintenanceBranch && params.FAIL_AT_END != 'NO') ||
                      (params.FAIL_AT_END == 'YES')) {
                        buildParamsAsArray.add('--fail-at-end')
                        fail_at_end = true
                    }

                    // Manage the EXTRA_BUILD_PARAMS
                    buildParamsAsArray.add(params.EXTRA_BUILD_PARAMS)
                    extraBuildParams = buildParamsAsArray.join(' ')

                    releaseVersion = pomVersion.split('-')[0]
                }
                ///////////////////////////////////////////
                // Updating build displayName and description
                ///////////////////////////////////////////
                script {
                    String user_name = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause').userId[0]
                    if ( user_name == null) { user_name = "auto" }

                    currentBuild.displayName = (
                      "#$currentBuild.number-$params.Action: $user_name"
                    )

                    // updating build description
                    currentBuild.description = ("""
                      $params.Action Build - fail_at_end: $fail_at_end ($params.FAIL_AT_END)
                      Sonar: $params.SONAR_ANALYSIS - Script: $hasPostLoginScript
                      Extra args: $hasExtraBuildArgs - Debug: $params.DEBUG_BEFORE_EXITING""".stripIndent()
                    )
                }
            }
        }

        stage('Prepare build') {
            steps {
                container(tsbiImage) {
                    script {
                        echo 'Git login'
                        withCredentials([gitCredentials]) {
                            sh """
                                bash .jenkins/git-login.sh \
                                    "\${GITHUB_LOGIN}" \
                                    "\${GITHUB_TOKEN}"
                            """
                        }

                        echo 'Docker login'
                        withCredentials([artifactoryCredentials]) {
                            /* In following sh step, '${ARTIFACTORY_REGISTRY}' will be replaced by groovy */
                            /* but the next two ones, "\${ARTIFACTORY_LOGIN}" and "\${ARTIFACTORY_PASSWORD}", */
                            /* will be replaced by the bash process. */
                            sh """
                                bash .jenkins/docker-login.sh \
                                    '${env.TALEND_REGISTRY}' \
                                    "\${ARTIFACTORY_LOGIN}" \
                                    "\${ARTIFACTORY_PASSWORD}"
                            """
                        }
                    }
                }
            }
        }

        stage('Post login') {
            // FIXME: this step is an aberration and a gaping security hole.
            //        As soon as the build is stable enough not to rely on this crutch, let's get rid of it.
            steps {
                container(tsbiImage) {
                    withCredentials([nexusCredentials, gitCredentials, artifactoryCredentials]) {
                        script {
                            if (params.POST_LOGIN_SCRIPT?.trim()) {
                                try {
                                    sh "bash -c '${params.POST_LOGIN_SCRIPT}'"
                                } catch (ignored) {
                                    // The job must not fail if the script fails
                                }
                            }
                        }
                    }
                }
            }
        }
        stage('Build') {
            when {
                expression { params.Action == 'STANDARD' }
            }
            steps {
                container(tsbiImage) {
                    script {
                        withCredentials([nexusCredentials
                                         , sonarCredentials]) {
                            sh """
                                bash .jenkins/build.sh \
                                    '${params.Action}' \
                                    '${isOnMasterOrMaintenanceBranch}' \
                                    '${params.SONAR_ANALYSIS}' \
                                    '${env.BRANCH_NAME}' \
                                    ${extraBuildParams}
                            """
                        }
                    }
                }
            }

            post {
                always {
                    recordIssues(
                        enabledForFailure: true,
                        tools: [
                            junitParser(
                                id: 'unit-test',
                                name: 'Unit Test',
                                pattern: '**/target/surefire-reports/*.xml'
                            )
                        ]
                    )
                }
            }
        }

        stage('Release') {
            when {
                expression { params.Action == 'RELEASE' }
            }
            steps {
                withCredentials([gitCredentials,
                                 nexusCredentials,
                                 artifactoryCredentials]) {
                    container(tsbiImage) {
                        script {
                            sh """
                                bash .jenkins/release.sh \
                                    '${params.Action}' \
                                    '${releaseVersion}' \
                                    ${extraBuildParams}
                            """
                        }
                    }
                }
            }
        }

        stage('Deploy') {
            when {
                expression { params.Action == 'DEPLOY' }
            }
            steps {
                withCredentials([nexusCredentials]) {
                    container(tsbiImage) {
                        script {
                            sh """
                                bash .jenkins/deploy.sh \
                                    '${params.Action}' \
                                    ${extraBuildParams}
                            """
                        }
                    }
                }
            }
        }

        stage('Debug') {
            when { expression { return params.DEBUG_BEFORE_EXITING } }
            steps { script { input message: 'Finish the job?', ok: 'Yes' } }
        }
    }
    post {
        success {
            script {
                //Only post results to Slack for Master and Maintenance branches
                if (isOnMasterOrMaintenanceBranch) {
                    slackSend(
                        color: '#00FF00',
                        message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
                        channel: "${slackChannel}")
                }
            }
            script {
                println "====== Publish Coverage"
                publishCoverage adapters: [jacocoAdapter("${_COVERAGE_REPORT_PATH}")]
            }
        }
        failure {
            script {
                //Only post results to Slack for Master and Maintenance branches
                if (isOnMasterOrMaintenanceBranch) {
                    //if previous build was a success, ping channel in the Slack message
                    if ("SUCCESS".equals(currentBuild.previousBuild.result)) {
                        slackSend(
                            color: '#FF0000',
                            message: "@here : NEW FAILURE: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
                            channel: "${slackChannel}")
                    } else {
                        //else send notification without pinging channel
                        slackSend(
                            color: '#FF0000',
                            message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
                            channel: "${slackChannel}")
                    }
                }
            }
        }
        always {
            container(tsbiImage) {
                recordIssues(
                    enabledForFailure: true,
                    tools: [
                        taskScanner(
                            id: 'disabled',
                            name: '@Disabled',
                            includePattern: '**/src/**/*.java',
                            ignoreCase: true,
                            normalTags: '@Disabled'
                        ),
                        taskScanner(
                            id: 'todo',
                            name: 'Todo(low)/Fixme(high)',
                            includePattern: '**/src/**/*.java',
                            ignoreCase: true,
                            highTags: 'FIX_ME, FIXME',
                            lowTags: 'TO_DO, TODO'
                        )
                    ]
                )
                script {
                    println '====== Archive artifacts'
                    println "Artifact 1: ${_ARTIFACT_COVERAGE}\\n"
                    archiveArtifacts artifacts: "${_ARTIFACT_COVERAGE}", allowEmptyArchive: true, onlyIfSuccessful: false
                }
            }
        }
    }
}
