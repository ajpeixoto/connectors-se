
//Imports
import java.util.regex.Matcher

// Credentials
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

// In PR environment, the branch name is not valid and should be swap with pr name.
final String pullRequestId = env.CHANGE_ID
final String branch_name = pullRequestId != null ? env.CHANGE_BRANCH : env.BRANCH_NAME

// Job config
final boolean isOnMasterOrMaintenanceBranch = branch_name == "master" || branch_name.startsWith("maintenance/")
final boolean isOnLocalesBranch = branch_name.startsWith("locales/")

// Job variables declaration
String branch_user
String branch_ticket
String branch_description
String pomVersion
String componentRuntimeVersion
String finalVersion
String extraBuildParams = ''
Boolean fail_at_end = false
String logContent
Boolean devBranch_mavenDeploy = false
final String repository = 'connectors-se'

// Files and folder definition
final String _COVERAGE_REPORT_PATH = '**/jacoco-aggregate/jacoco.xml'

// Artifacts paths
final String _ARTIFACT_COVERAGE = '**/target/site/**/*.*'
final String _ARTIFACT_BUILD_LOGS  = '**/build_log.txt'
final String _ARTIFACT_RAW_LOGS   = '**/raw_log.txt'

pipeline {
    agent {
        kubernetes {
            yamlFile '.jenkins/jenkins_pod.yml'
            defaultContainer 'main'
        }
    }

    environment {
        // This jenkins job need some environments variables to be defined
        // Common variables are defined in flux jenkins.yaml at config-vars/jenkins/globalNodeProperties/envVars
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
        TESTCONTAINERS_RYUK_DISABLED = 'true' // Ensure proper test execution for tests using testcontainers.
    }

    options {
        buildDiscarder(
          logRotator(
            artifactNumToKeepStr: '5',
            numToKeepStr: isOnMasterOrMaintenanceBranch ? '10' : '7'
          )
        )
        timeout(time: 2, unit: 'HOURS')
        skipStagesAfterUnstable()
    }

    triggers {
        cron(branch_name == "master" ? "0 0 * * *" : "")
    }

    parameters {
        choice(
          name: 'ACTION',
          choices: ['STANDARD', 'RELEASE'],
          description: '''
            Kind of run: 
            - STANDARD : (default) classic CI build
            - RELEASE : Build release, deploy to the Nexus for master/maintenance branches''')
        booleanParam(
          name: 'MAVEN_DEPLOY',
          defaultValue: !isOnLocalesBranch,
          description: '''
            Deploy A build to the Nexus after the build
            Default is true for any branches except "locales/"''')
        string(
          name: 'VERSION_QUALIFIER',
          defaultValue: 'DEFAULT',
          description: '''
            Only for dev branches. It will build/deploy jars with the given version qualifier.
             - DEFAULT means the qualifier will be the Jira id extracted from the branch name.
            From "user/JIRA-12345_some_information" the qualifier will be 'JIRA-12345'.
            Before the build, the maven version will be set to: x.y.z-JIRA-12345-SNAPSHOT''')
        booleanParam(
          name: 'UPDATE_SNAPSHOT',
          defaultValue: true,
          description: '''
            UPDATE_SNAPSHOT : Add the --update-snapshot (-U) option to all maven cmd.
            Uncheck may lead not to have the last version of deployed connectors-se''')
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
        booleanParam(
          name: 'CHECKSTYLE',
          defaultValue: true,
          description: '''
            Execute checkstyle analysis (only for STANDARD action).
              - Code rules
              - Translation check             
            ''')
        string(
          name: 'EXTRA_BUILD_PARAMS',
          defaultValue: "",
          description: 'Add some extra parameters to maven commands. Applies to all maven calls.')
        string(name: 'POST_LOGIN_SCRIPT',
          defaultValue: "",
          description: 'Execute a shell command after login. Useful for maintenance.')
        booleanParam(
          name: 'JENKINS_DEBUG',
          defaultValue: false,
          description: '''
            Add an extra comportment to the job allowing to extra analysis:
              - keep the pod alive for debug purposes at the end
              - activate the Maven dependencies analysis stage''')
        booleanParam(
          name: 'DRAFT_CHANGELOG',
          defaultValue: true,
          description: '''
            Create a draft release changelog. User will need to approve it on github.
            Only used on release action''')
    }

    stages {
        stage('Validate parameters') {
            steps {
                ///////////////////////////////////////////
                // asdf install
                ///////////////////////////////////////////
                script {
                    println "asdf install the content of repository .tool-versions'\n"
                    sh 'bash .jenkins/asdf_install.sh'
                }
                ///////////////////////////////////////////
                // Variables init
                ///////////////////////////////////////////
                script {
                    devBranch_mavenDeploy = !isOnMasterOrMaintenanceBranch && params.MAVEN_DEPLOY
                }
                ///////////////////////////////////////////
                // Pom version and Qualifier management
                ///////////////////////////////////////////
                script {
                    final def pom = readMavenPom file: 'pom.xml'
                    pomVersion = pom.version
                    componentRuntimeVersion = pom.properties['component-runtime.version']

                    if (params.ACTION == 'RELEASE' && !pomVersion.endsWith('-SNAPSHOT')) {
                        error('Cannot release from a non SNAPSHOT, exiting.')
                    }

                    if (params.ACTION == 'RELEASE' && !branch_name.startsWith('maintenance/')) {
                        error('Can only release from a maintenance branch, exiting.')
                    }

                    println 'Manage the version qualifier'
                    if (devBranch_mavenDeploy || (params.VERSION_QUALIFIER != ("DEFAULT"))) {
                        println """
                             We need to add qualifier in followings cases:
                             - We are on a deployed dev branch
                             - We do not want to deploy but give a qualifier
                             """.stripIndent()

                        branch_user = ""
                        branch_ticket = ""
                        branch_description = ""

                        if (params.VERSION_QUALIFIER != ("DEFAULT")) {
                            // If the qualifier is given, use it
                            println """
                             No need to detect qualifier, use the given one: "$params.VERSION_QUALIFIER"
                             """.stripIndent()
                        }
                        else {
                            echo "Validate the branch name"
                            (branch_user,
                            branch_ticket,
                            branch_description) = extract_branch_info(branch_name)

                            // Check only branch_user, because if there is an error all three params are empty.
                            if (branch_user == ("")) {
                                currentBuild.description = ("ERROR: The branch name is not correct")
                                println("""
                                ERROR: The branch name doesn't comply with the format: user/JIRA-1234-Description  
                                It is MANDATORY for artifact management.  
                                You have few options:  
                                - You do not need to deploy, uncheck MAVEN_DEPLOY checkbox  
                                - Change the VERSION_QUALIFIER text box to a personal qualifier,  
                                  BUT you need to do it on ALL se/ee and cloud-components build  
                                - Rename your branch  
                                """.stripIndent())
                                error("ERROR: The branch name is not correct")
                            }
                        }

                        echo "Insert a qualifier in pom version..."
                        finalVersion = add_qualifier_to_version(
                          pomVersion,
                          branch_ticket,
                          "$params.VERSION_QUALIFIER" as String)

                        echo """
                          Configure the version qualifier for the curent branche: $branch_name
                          requested qualifier: $params.VERSION_QUALIFIER
                          with User = $branch_user, Ticket = $branch_ticket, Description = $branch_description
                          Qualified Version = $finalVersion"""
                    }
                    else {
                        println "No need to add qualifier on this job"
                        finalVersion = pomVersion
                    }

                    println 'Manage the FAIL_AT_END parameter'
                    if ((isOnMasterOrMaintenanceBranch && params.FAIL_AT_END != 'NO') ||
                      (params.FAIL_AT_END == 'YES')) {
                        fail_at_end = true
                    }
                }
                ///////////////////////////////////////////
                // Updating build displayName and description
                ///////////////////////////////////////////
                script {
                    String jenkins_action
                    if(params.ACTION == 'STANDARD' && params.MAVEN_DEPLOY) {
                        jenkins_action = 'DEPLOY'
                    }
                    else {
                        jenkins_action = params.ACTION
                    }

                    job_name_creation("$jenkins_action")

                    // updating build description
                    String description = """
                      $finalVersion - $jenkins_action 
                      Component-runtime Version: $componentRuntimeVersion  
                      Sonar: $params.SONAR_ANALYSIS  
                      Extra user maven args:  `$params.EXTRA_BUILD_PARAMS`  
                      Post login script: ```$params.POST_LOGIN_SCRIPT```  
                      Maven fail-at-end activation: $fail_at_end ($params.FAIL_AT_END)  
                      Debug: $params.JENKINS_DEBUG  
                      """.stripIndent()
                    job_description_append(description)

                    String tool_versions = readFile ".tool-versions"
                    job_description_append(
                      ".tool-versions content: ${tool_versions.replace('\n',"; ")}")
                }
            }
        }

        stage('Prepare build') {
            steps {
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

                    // On development branches the connector version shall be edited for deployment
                    if (!isOnMasterOrMaintenanceBranch) {
                        // Maven documentation about maven_version:
                        // https://docs.oracle.com/middleware/1212/core/MAVEN/maven_version.htm
                        sh "bash .jenkins/mvn_set_versions.sh ${finalVersion}"
                    }

                    // build extraBuildParams
                    extraBuildParams = extraBuildParams_assembly(fail_at_end,
                                                                 params.UPDATE_SNAPSHOT as Boolean,
                                                                 isOnMasterOrMaintenanceBranch)

                    job_description_append("Final parameters used for maven:  ")
                    job_description_append("`$extraBuildParams`")

                    if (!isOnMasterOrMaintenanceBranch) {
                        pom_project_property_edit()
                    }
                }
            }
            post {
                always {
                    println "Artifact Poms files after edition"
                    archiveArtifacts artifacts: '**/*pom.*', allowEmptyArchive: false, onlyIfSuccessful: false
                }
            }
        }

        stage('Post login') {
            // FIXME: this step is an aberration and a gaping security hole.
            //        As soon as the build is stable enough not to rely on this crutch, let's get rid of it.
            steps {
                withCredentials([nexusCredentials,
                                 gitCredentials,
                                 artifactoryCredentials]) {
                    script {
                        try {
                            //Execute content of Post Login Script parameter
                            if (params.POST_LOGIN_SCRIPT?.trim()) {
                                sh "bash -c '${params.POST_LOGIN_SCRIPT}'"
                            }
                        } catch (ignored) {
                            // The job must not fail if the script fails
                            echo 'Failure caught during Post Login and ignored'
                        }
                        echo 'End of post login scripts'
                    }
                }
            }
        }

        stage('Maven dependencies analysis') {
            when {
                expression { params.JENKINS_DEBUG }
            }
            steps {
                withCredentials([nexusCredentials,
                                 artifactoryCredentials]) {
                    script {
                        println 'Debug step to resolve pom file and analysis'
                        sh """\
                            #!/usr/bin/env bash 
                            set -xe
                            (mvn help:effective-pom | tee effective-pom-se.txt) &&\
                            (mvn dependency:tree | tee dependency-tree-se.txt)
                        """
                    }
                }
            }
            post {
                always {
                    println "Artifact effective-pom and dependency:tree"
                    archiveArtifacts artifacts: "effective-pom-se.txt", allowEmptyArchive: false, onlyIfSuccessful: false
                    archiveArtifacts artifacts: "dependency-tree-se.txt", allowEmptyArchive: false, onlyIfSuccessful: false
                }
            }
        }

        stage('Maven validate to install') {
            when {
                expression { params.ACTION == 'STANDARD' }
            }
            steps {
                script {
                    withCredentials([nexusCredentials]) {
                        realtimeJUnit(
                            testResults: '**/target/failsafe-reports/*.xml') {
                            realtimeJUnit(
                                testResults: '**/target/surefire-reports/*.xml') {

                                // checkstyle is skipped because executed dedicated stage
                                sh """
                                    bash .jenkins/mvn_build.sh \
                                        --define checkstyle.skip=true \
                                        ${extraBuildParams}
                                """
                            }
                        }
                    }
                }
            }

            post {
                always {
                    junit '**/target/surefire-reports/*.xml, **/target/failsafe-reports/*.xml' // Needed for TTO
                    recordIssues(
                      enabledForFailure: true,
                      tools: [
                        junitParser(
                          id: 'unit-test',
                          name: 'Unit Test',
                          pattern: '**/target/surefire-reports/*.xml'
                        ),
                        junitParser(
                          id: 'integration-test',
                          name: 'Integration Test',
                          pattern: '**/target/failsafe-reports/*.xml'
                        )
                      ]
                    )
                }
            }
        }

        stage('Maven checkstyle') {
            when {
                // For dependencies reasons, checkstyle need to be after the build
                // If not, the qualified dependencies doesn't exist yet
                expression { params.CHECKSTYLE }
            }
            steps {
                script {
                    withCredentials([nexusCredentials]) {
                        sh """
                            bash .jenkins/mvn_checkstyle.sh ${extraBuildParams}
                        """
                    }
                }
            }
            post {
                always {
                    script {
                        println '====== Archive artifacts'
                        println "Checkstyle report"
                        archiveArtifacts artifacts: "**/checkstyle-result.xml", allowEmptyArchive: true, onlyIfSuccessful: false
                    }

                    recordIssues(
                      enabledForFailure: true,
                      tools: [
                        checkStyle()
                      ]
                    )
                }
            }
        }

        stage('Maven deploy') {
            when {
                expression { params.ACTION == 'STANDARD' && params.MAVEN_DEPLOY }
            }
            steps {
                withCredentials([nexusCredentials]) {
                    script {
                        sh """
                            bash .jenkins/mvn_deploy.sh \
                                ${extraBuildParams}
                        """
                    }
                }
            }
        }

        stage('Maven sonar') {
            when {
                expression { params.ACTION == 'STANDARD' && params.SONAR_ANALYSIS }
            }
            steps {
                script {
                    withCredentials([nexusCredentials,
                                     sonarCredentials,
                                     gitCredentials]) {

                        if (pullRequestId != null) {

                            println 'Run analysis for PR'
                            sh """\
                            bash .jenkins/mvn_sonar_pr.sh \
                                '${branch_name}' \
                                '${env.CHANGE_TARGET}' \
                                '${pullRequestId}' \
                                ${extraBuildParams}
                            """.stripIndent()
                        } else {
                            echo 'Run analysis for branch'
                            sh """\
                            bash .jenkins/mvn_sonar_branch.sh \
                            '${branch_name}' \
                            ${extraBuildParams}
                            """.stripIndent()
                        }

                    }
                }
            }
        }

        stage('Release') {
            when {
                expression { params.ACTION == 'RELEASE' }
            }
            steps {
                withCredentials([gitCredentials,
                                 nexusCredentials,
                                 artifactoryCredentials]) {
                    script {
                        String releaseVersion = pomVersion.split('-')[0]
                        sh """
                            bash .jenkins/release.sh \
                                'RELEASE' \
                                '${releaseVersion}' \
                                ${extraBuildParams}
                        """
                    }
                }
            }
        }

        stage('Release changelog') {
            when {
                expression { params.ACTION == 'RELEASE' }
            }
            steps {
                withCredentials([gitCredentials]) {
                    // Do not failed the build in case of changelog issue.
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        script {
                            String releaseVersion = pomVersion.split('-')[0]
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
    post {
        always {
            script{
                logContent = extractJenkinsLog()
            }

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
                ),
                //Add Maven log analysis in jenkins build result page
                // It allows to see the all, non blocking warning in our build log to reduce them in the future.
                mavenConsole()
              ]
            )
            script {
                println '====== Archive artifacts'
                println "Coverage reports: ${_ARTIFACT_COVERAGE}"
                archiveArtifacts artifacts: "${_ARTIFACT_COVERAGE}", allowEmptyArchive: true, onlyIfSuccessful: false
                println "Build logs: ${_ARTIFACT_BUILD_LOGS}"
                archiveArtifacts artifacts: "${_ARTIFACT_BUILD_LOGS}", allowEmptyArchive: true, onlyIfSuccessful: false
                // TODO When log will be safe, remove the row version (which just have extra ANSI escape codes)
                println "Build raw logs: ${_ARTIFACT_RAW_LOGS}"
                archiveArtifacts artifacts: "${_ARTIFACT_RAW_LOGS}", allowEmptyArchive: true, onlyIfSuccessful: false
            }

            script {
                if (params.JENKINS_DEBUG) {
                    jenkinsBreakpoint()
                }
            }
        }
        success {
            script {
                //Only post results to Slack for Master and Maintenance branches
                if (isOnMasterOrMaintenanceBranch) {
                    slackSend(
                      color: '#00FF00',
                      message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
                      channel: "${env.SLACK_CI_CHANNEL}")
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
                    if ("SUCCESS" == (currentBuild.previousBuild.result)) {
                        slackSend(
                          color: '#FF0000',
                          message: "@here : NEW FAILURE: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
                          channel: "${env.SLACK_CI_CHANNEL}")
                    } else {
                        //else send notification without pinging channel
                        slackSend(
                          color: '#FF0000',
                          message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
                          channel: "${env.SLACK_CI_CHANNEL}")
                    }
                }
            }

            script {
                CleanM2Corruption(logContent)
            }
        }
    }
}

/**
 * Job name creation
 * Update the job name in a predefined format, including build launcher user name
 *
 * @param extra: extra string to include in the job name
 * @return void
 */
private void job_name_creation(String extra='') {
    String user_name = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause').userId[0]
    if (user_name == null) {
        user_name = "auto"
    }

    String comment = ''
    if ('' != extra){
        comment = "-$extra"
    }

    currentBuild.displayName = (
      "#$currentBuild.number$comment: $user_name"
    )
}

/**
 * Append a new line to job description
 * REM This is MARKDOWN, do not forget double space at the end of line
 *
 * @param new line
 * @return void
 */
private void job_description_append(String new_line) {
    if (currentBuild.description == null) {
        println "Create the job description with: \n$new_line"
        currentBuild.description = new_line
    } else {
        println "Edit the job description adding: $new_line"
        currentBuild.description = currentBuild.description + '\n' + new_line
    }
}

/**
 * Implement a simple breakpoint to stop actual job
 * Use the method anywhere you need to stop
 * The first usage is to keep the pod alive on post stage.
 * Change and restore the job description to be more visible
 *
 * @param none
 * @return void
 */
private void jenkinsBreakpoint() {
    // Backup the description
    String job_description_backup = currentBuild.description
    // updating build description
    currentBuild.description = "ACTION NEEDED TO CONTINUE \n ${job_description_backup}"
    // Request user action
    input message: 'Finish the job?', ok: 'Yes'
    // updating build description
    currentBuild.description = "$job_description_backup"
}

/**
 * Extract actual jenkins job log content, store it in:
 *   - global variable "logContent"
 *   - "raw_log.txt" file
 *   - cleaned in "build_log.txt"
 *   *
 * @param None
 * @return logContent as string
 */
private String extractJenkinsLog() {

    println "Extract the jenkins log file"
    String newLog = Jenkins.getInstance().getItemByFullName(env.JOB_NAME)
      .getBuildByNumber(env.BUILD_NUMBER.toInteger())
      .logFile.text
    // copy the log in the job's own workspace
    writeFile file: "raw_log.txt", text: newLog

    // Clean jenkins log file, could do better with a "ansi2txt < raw_log.txt" instead of "cat raw_log.txt"
    // https://en.wikipedia.org/wiki/ANSI_escape_code
    // Also could be good to replace '8m0m' by '' when common lib will be in place
    sh """\
      #!/usr/bin/env bash 
      set -xe
      cat raw_log.txt | col -b | sed 's;ha:////[[:print:]]*AAAA[=]*;;g' > build_log.txt
    """

    return newLog
}

/**
 * Clean m2 folder from corrupted file if needed.
 * Created after ticket TDI-48532
 * TODO: https://jira.talendforge.org/browse/TDI-48913 Centralize script for Jenkins M2 Corruption clean
 * @param String logContent
 *
 * @return void
 */
private void CleanM2Corruption(String logContent) {

    println 'Checking for Malformed encoding error'
    if (logContent.contains("Malformed \\uxxxx encoding")) {
        println 'Malformed encoding detected: Cleaning M2 corruptions'
        try {
            sh """\
            #!/usr/bin/env bash 
            set -xe
            grep --recursive --word-regexp --files-with-matches --regexp '\\u0000' ~/.m2/repository | xargs -I % rm %
        """
        }
        catch (ignored) {
            // The stage must not fail if grep returns no lines.
        }
    }
}

/**
 * Assembly all needed items to put inside extraBuildParams
 *
 * @param Boolean fail_at_end, if set to true, --fail-at-end will be added
 * @param Boolean snapshot_update, if set to true, --update-snapshots will be added
 * @param Boolean use_light_maven_enforcer, if set to true, --define use-maven-enforcer-light-rules
 *
 * @return extraBuildParams as a string ready for mvn cmd
 */
private String extraBuildParams_assembly(Boolean fail_at_end,
                                         Boolean snapshot_update,
                                         Boolean use_light_maven_enforcer) {
    String extraBuildParams

    println 'Processing extraBuildParams'

    println 'Manage the env.MAVEN_SETTINGS and env.DECRYPTER_ARG'
    final List<String> buildParamsAsArray = ['--settings',
                                             env.MAVEN_SETTINGS,
                                             env.DECRYPTER_ARG]

    println 'Manage the EXTRA_BUILD_PARAMS'
    buildParamsAsArray.add(params.EXTRA_BUILD_PARAMS)

    println 'Manage the failed-at-end option'
    if (fail_at_end) {
        buildParamsAsArray.add('--fail-at-end')
    }

    println 'Manage the --update-snapshots option'
    if (snapshot_update) {
        buildParamsAsArray.add('--update-snapshots')
    }

    println 'Manage the maven-enforcer profile'
    if (use_light_maven_enforcer) {
        buildParamsAsArray.add('--define use-maven-enforcer-light-rules')
    }

    println 'Construct extraBuildParams'

    extraBuildParams = buildParamsAsArray.join(' ')
    println "extraBuildParams: $extraBuildParams"

    return extraBuildParams
}

/**
 * Edit properties in the pom to allow maven to choose between qualifier version or normal one
 *
 * @return nothing
 * it will edit local pom on specific properties
 */
private void pom_project_property_edit() {

    println 'No maven property to update'

    // This step is a reminder of where to put this action if needed
    // It is not needed in connectors-se for now but in connectors-ee and cloud-components jobs.
}

/**
 * create a new version from actual one and given jira ticket or user qualifier
 * Priority to user qualifier
 *
 * The branch name has comply with the format: user/JIRA-1234-Description
 * It is MANDATORY for artifact management.
 *
 * @param String version actual version to edit
 * @param GString ticket
 * @param GString user_qualifier to be checked as priority qualifier
 *
 * @return String new_version with added qualifier
 */
private static String add_qualifier_to_version(String version, String ticket, String user_qualifier) {
    String new_version

    if (user_qualifier.contains("DEFAULT")) {
        if (version.contains("-SNAPSHOT")) {
            new_version = version.replace("-SNAPSHOT", "-$ticket-SNAPSHOT" as String)
        } else {
            new_version = "$version-$ticket".toString()
        }
    } else {
        new_version = version.replace("-SNAPSHOT", "-$user_qualifier-SNAPSHOT" as String)
    }
    return new_version
}

/**
 * extract given branch information
 *
 * The branch name has comply with the format: user/JIRA-1234-Description
 * It is MANDATORY for artifact management.
 *
 * @param branch_name row name of the branch
 *
 * @return A list containing the extracted: [user, ticket, description]
 * The method also raise an assert exception in case of wrong branch name
 */
private static ArrayList<String> extract_branch_info(String branch_name) {

    String branchRegex = /^(?<user>.*)\/(?<ticket>[A-Z]{2,8}-\d{1,6})[_-](?<description>.*)/
    Matcher branchMatcher = branch_name =~ branchRegex

    try {
        assert branchMatcher.matches()
    }
    catch (AssertionError ignored) {
        return ["", "", ""]
    }

    String user = branchMatcher.group("user")
    String ticket = branchMatcher.group("ticket")
    String description = branchMatcher.group("description")

    return [user, ticket, description]
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