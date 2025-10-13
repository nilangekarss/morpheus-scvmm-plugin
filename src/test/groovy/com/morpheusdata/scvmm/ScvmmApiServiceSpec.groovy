package com.morpheusdata.scvmm

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.MorpheusServices
import com.bertramlabs.plugins.karman.CloudFile
import spock.lang.Specification
import io.reactivex.rxjava3.core.Single
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.core.synchronous.compute.MorpheusSynchronousComputeServerService
import com.morpheusdata.core.synchronous.MorpheusSynchronousFileCopyService

class ScvmmApiServiceSpec extends Specification {


    def "test transferImage transfers image successfully"() {
        given:
        def fileCopyService = Stub(MorpheusSynchronousFileCopyService)
        def mockedComputerServer = Mock(ComputeServer)
        def computeServerService = Mock(MorpheusSynchronousComputeServerService)
        computeServerService.get(_) >> mockedComputerServer
        def morpheusServices = Mock(MorpheusServices) {
            getFileCopy() >> {
                return fileCopyService
            }
            getComputeServer() >> computeServerService
        }

        def morpheusContext = Mock(MorpheusContext) {
            getServices() >> morpheusServices
        }

        def apiService = new ScvmmApiService(morpheusContext)

        // After creating morpheusContext mock
        morpheusContext.executeWindowsCommand(*_) >> Single.just([success: true,
                                                                  data: "{\"Mode\":\"d-----\",\"Name\":\"testImage\",\"Attributes\":\"Directory\"}"])

        // Prepare test data
        def opts = [
                zoneRoot: "C:\\Temp",
               // hypervisor: "hypervisor1",
                sshPort: '22',
                sshHost: 'localhost',
                sshUsername: 'admin',
                sshPassword: 'password'
        ]
        // Add this before using opts
        opts.hypervisor = mockedComputerServer
        def inputStreamData = Mock(InputStream)
        //def inputStreamData = new ByteArrayInputStream("VHD".bytes)
        def metadataFile = Mock(CloudFile)
        metadataFile.name >> "metadata.json"
        metadataFile.inputStream >> inputStreamData
        metadataFile.contentLength >> 3L

        def cloudFiles = [metadataFile]
        def imageName = "testImage"
        def serviceResp =  new ServiceResponse(success: true)

        fileCopyService.copyToServer(_,_,_,_,_,_,_) >> {
            println("copyToServer called with args: ${it}")
            return serviceResp
        }

        when:
        def result = apiService.transferImage(opts, cloudFiles, imageName)

        then:
        result.success == true
    }

    def "test stopServer successfully stops server"() {
        given:
        def morpheusContext = Mock(MorpheusContext)
        def apiService = new ScvmmApiService(morpheusContext)
        def server = Mock(ComputeServer) {
            id >> 1L
            name >> "test-vm"
        }
        def opts = [
                zoneRoot: "C:\\Temp",
                hypervisor: server,
                sshHost: 'localhost',
                sshUsername: 'admin',
                sshPassword: 'password'
        ]

        morpheusContext.executeWindowsCommand(*_) >> Single.just([success: true, data: '{"Status":"Success"}'])

        when:
        def result = apiService.stopServer(opts, server)

        then:
        result.success == true
    }


    def "test deleteIso successfully deletes ISO file"() {
        given:
        def morpheusContext = Mock(MorpheusContext)
        def apiService = new ScvmmApiService(morpheusContext)
        def opts = [
                zoneRoot: "C:\\Temp",
                sshHost: 'localhost',
                sshUsername: 'admin',
                sshPassword: 'password'
        ]
        def isoPath = "C:\\Temp\\isos\\test.iso"

        morpheusContext.executeWindowsCommand(*_) >> Single.just([success: true, data: '{"Status":"Success"}'])

        when:
        def result = apiService.deleteIso(opts, isoPath)

        then:
        result.success == true
    }


}