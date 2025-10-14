package com.morpheusdata.scvmm

import com.bertramlabs.plugins.karman.CloudFile
import com.morpheusdata.core.MorpheusAsyncServices
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.MorpheusServices
import com.morpheusdata.core.MorpheusVirtualImageService
import com.morpheusdata.core.cloud.MorpheusCloudService
import com.morpheusdata.core.synchronous.MorpheusSynchronousVirtualImageService
import com.morpheusdata.core.synchronous.compute.MorpheusSynchronousComputeServerService
import com.morpheusdata.core.synchronous.MorpheusSynchronousStorageVolumeService
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.network.MorpheusNetworkService
import com.morpheusdata.core.synchronous.library.MorpheusSynchronousWorkloadTypeService
import com.morpheusdata.model.*
import com.morpheusdata.model.provisioning.UserConfiguration
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.model.ComputeServerInterface
import com.morpheusdata.model.Workload
import com.morpheusdata.model.WorkloadType
import com.morpheusdata.model.Instance
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Account
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.OsType
import com.morpheusdata.model.VirtualImageLocation
import com.morpheusdata.response.ProvisionResponse
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonOutput
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import spock.lang.Specification

class ScvmmProvisionProviderRunWorkloadSpec extends Specification {
    private MorpheusContext morpheusContext
    private ScvmmPlugin plugin
    private ScvmmProvisionProvider provisionProvider
    private ScvmmApiService mockApiService
    private MorpheusSynchronousComputeServerService computeServerService
    private MorpheusSynchronousWorkloadTypeService workloadTypeService
    private MorpheusSynchronousStorageVolumeService storageVolumeService
    private MorpheusSynchronousVirtualImageService virtualImageService
    private MorpheusVirtualImageService asyncVirtualImageService
    //private ComputeServer mockedComputerServer
    //private WorkloadType mockedWorkloadType
    def setup() {
        // Setup mock context and services
        morpheusContext = Mock(MorpheusContext)
        plugin = Mock(ScvmmPlugin)

        // Mock services
        computeServerService = Mock(MorpheusSynchronousComputeServerService)
        def cloudService = Mock(MorpheusCloudService)
        def networkService = Mock(MorpheusNetworkService)
        workloadTypeService = Mock(MorpheusSynchronousWorkloadTypeService)
        storageVolumeService = Mock(MorpheusSynchronousStorageVolumeService)
        virtualImageService = Mock(MorpheusSynchronousVirtualImageService)
        asyncVirtualImageService = Mock(MorpheusVirtualImageService)

        def morpheusServices = Mock(MorpheusServices) {
            getComputeServer() >> computeServerService
            getWorkloadType() >> workloadTypeService
            getStorageVolume() >> storageVolumeService
            getVirtualImage() >> virtualImageService
        }
        def morpheusAsyncServices = Mock(MorpheusAsyncServices) {
            getCloud() >> cloudService
            getNetwork() >> networkService
            getStorageVolume() >> storageVolumeService
            getVirtualImage() >> asyncVirtualImageService
        }

        // Configure context mocks
        morpheusContext.getAsync() >> morpheusAsyncServices
        morpheusContext.getServices() >> morpheusServices

        // Create and inject mock API service
//        mockApiService = Mock(ScvmmApiService)
//        provisionProvider = new ScvmmProvisionProvider(plugin, morpheusContext)
//        provisionProvider.apiService = mockApiService

        mockApiService = Mock(ScvmmApiService)
        provisionProvider = Spy(ScvmmProvisionProvider, constructorArgs: [plugin, morpheusContext])
        provisionProvider.apiService = mockApiService
    }

    def storageVols() {
        def vol1 = new StorageVolume(
                displayOrder: 0,
                name: "root",
                datastoreOption: "auto",
                rootVolume: true,
                id: 507,
                resizeable: true,
                shortName: "root",
                maxStorage: 42949672960
        )
        def vol2 = new StorageVolume(
                displayOrder: 2,
                name: "data-2",
                rootVolume: false,
                id: 509,
                resizeable: true,
                shortName: "data-2",
                maxStorage: 9663676416
        )
        vol1.setExternalId("external-id-1")
        vol1.setExternalId("external-id-2")
        return [vol1, vol2]
    }

    def configsMap = [
            maxMemory: 4294967296,
            maxStorage: 71940702208,
            maxCpu: 0,
            maxCores: 1,
            maxGpus: 0,
            template: 168,
            backup: [
                    backupJob: [
                            syntheticFullEnabled: false,
                            retentionCount: null,
                            syntheticFullSchedule: null,
                            scheduleTypeId: null
                    ],
                    veeamManagedServer: "",
                    createBackup: false,
                    jobAction: "new",
                    providerBackupType: -1
            ],
            isVpcSelectable: true,
            customOptions: [:],
            hostId: 2,
            createBackup: true,
            hasNoUser: false,
            expose: [],
            lbInstances: [],
            isEC2: false,
            resourcePoolId: "",
            layoutSize: 1,
            createUser: true,
            memoryDisplay: "MB",
            scvmmCapabilityProfile: "Hyper-V",
            noAgent: true,
            vm: true,
            networkInterfaces: [
                    [
                            id: "network-1",
                            network: [
                                    id: 1,
                                    group: null,
                                    subnet: null,
                                    dhcpServer: true,
                                    name: "vlanbaseVmNetwork",
                                    pool: null
                            ],
                            ipAddress: null,
                            macAddress: null,
                            networkInterfaceTypeId: null,
                            networkInterfaces: [],
                            ipMode: null
                    ]
            ],
            volumes: [
                    // Volume details included here...
                    [
                            displayOrder: 0,
                            name: "root",
                            datastoreOption: "auto",
                            rootVolume: true,
                            id: 507,
                            resizeable: true,
                            shortName: "root",
                            maxStorage: 42949672960
                    ],
                    [
                            displayOrder: 2,
                            name: "data-2",
                            rootVolume: false,
                            id: 509,
                            resizeable: true,
                            shortName: "data-2",
                            maxStorage: 9663676416
                    ]
            ],
            storageController: [],
            provisionPoweredOff: false,
            skipNetworkWait: false,
            userData: null,
            resourcePool: "",
            hosts: [container35: "127.0.0.1"],
            evars: [:]
    ]

    def "test runWorkload successful VM creation"() {
        given:
        // Create concrete objects instead of mocks where possible
        def cloud = new Cloud(id: 1L, name: "test-cloud")
        def account = new Account(id: 1L, name: "test-account")
        cloud.account = account

        def osType = new OsType(platform: "linux")
        def virtualImage = new VirtualImage(id: 2L, name: "test-image",
                refType: "ComputeZone", remotePath: "somePath", osType: osType,
                externalId: "external-id-1")

        def servicePlan = new ServicePlan(id: 3L, maxMemory: 42949672960L, maxCores: 2)
        def instance = new Instance(id: 4L, name: "test-instance", plan: servicePlan)

        // Create ComputeServer with concrete values
        def computerServer = new ComputeServer(
                id: 1L,
                name: "test-server",
                externalId: "vm-123",
                cloud: cloud,
                sourceImage: virtualImage,
                volumes: storageVols(),
                interfaces: []
        )

        def workloadType = new WorkloadType(refId: 1L, code: "test-workload-type")
        workloadType.setId(19L)

        String configsJson = JsonOutput.toJson(configsMap)


        // Create workload with concrete values
        def workload = new Workload(
                id: 5L,
                internalName: "testWorkload",
                server: computerServer,
                workloadType: workloadType,
                instance: instance,
                //sourceImage: virtualImage,
                //volumes: []
        )

        workload.configs = configsJson

        // Create UserConfiguration
        def userConfig = new UserConfiguration(username: "user", password: "pass")

        // Create WorkloadRequest with concrete values
        def workloadRequest = new WorkloadRequest()
        workloadRequest.usersConfiguration = [createUsers: [userConfig]]
        workloadRequest.cloudConfigOpts = [:]

        def opts = [noAgent: true]


        // Setup required response data
        def scvmmOpts = [
                account: account,
                zoneConfig: [libraryShare: "\\\\server\\share", diskPath: "C:\\Disks", workingPath: "C:\\VMs"],
                zone: cloud,
                zoneId: cloud.id,
                publicKey: "ssh-rsa AAAAB...",
                privateKey: "-----BEGIN RSA PRIVATE KEY-----\nMIIEpA...",
                rootSharePath: "\\\\server\\share",
                regionCode: "default"
        ]

        def controllerServer = new ComputeServer(
                id: 10L,
                name: "controller-01",
                serverType: new ComputeServerType(code: "scvmm-controller"),
                computeServerType: new ComputeServerType(code: "scvmm-controller"),
                sshHost: "10.0.0.5",
                sshUsername: "admin",
                sshPassword: "password123"
        )

        def datastore = new Datastore(id: 5L, name: "datastore1", externalId: "ds-123")
        def node = new ComputeServer(id: 2L, name: "node-01")

        def diskSpec = [
                type: "IDE",
                bus: "0",
                lun: "0",
                fileName: "testVM-disk1.vhdx",
                command: "\$ignore = New-SCVirtualDiskDrive -VMMServer localhost -IDE -Bus 0 -LUN 0 -JobGroup guid -VirtualHardDiskSizeMB 40960 -CreateDiffDisk \$false -Dynamic -FileName \"testVM-disk1.vhdx\" -Path \"C:\\Disks\" -VolumeType None"
        ]

        def provisionResponse = [
                serverUuid: "server-uuid",
                success: true,
                externalId: "vm-12345",
                publicIp: "10.0.1.100",
                privateIp: "192.168.1.100",
                hostname: "testVM"
        ]


        // Define expected response from getHostAndDatastore
        def hostAndDatastoreResponse = [node, datastore, "something", false]

        def mockedControllerOpts = [
                hypervisorConfig: [diskPath: "C:\\VMs\\Disks", workingPath: "C:\\VMs"],
                hypervisor: controllerServer,
                sshHost: "10.0.0.5",        // Using controllerNode.sshHost or default
                sshUsername: "admin",       // Using controllerNode.sshUsername or default
                sshPassword: "password123", // Using controllerNode.sshPassword or default
                zoneRoot: "C:\\VMs",        // Working path
                diskRoot: "C:\\VMs\\Disks"  // Disk path
        ]

        // First create a mock for the VirtualImageFiles
        def mockCloudFile = Mock(CloudFile)
        mockCloudFile.getName() >> "ubuntu-22.04.vhdx"

        def mockedCloudFiles = [mockCloudFile]
        // Create a Single that returns the list
        def mockFilesSingle = Single.just(mockedCloudFiles)


        provisionProvider.getHostAndDatastore(_, _, _, _, _, _, _, _, _) >> {
                return hostAndDatastoreResponse
        }

        provisionProvider.getDiskExternalIds(_, _) >> {
            def rtn = []
            rtn << [rootVolume: true, externalId: "external-id-1", idx: 0]
            rtn << [rootVolume: false, externalId: "external-id-2", idx: 1 ]
            return rtn
        }
        provisionProvider.additionalTemplateDisksConfig(_, _) >> {
            def additionalTemplateDisks = []
            additionalTemplateDisks << [idx: 1, diskCounter: 1, diskSize: 9663676416, busNumber: 0]
            return additionalTemplateDisks
        }
        provisionProvider.findVmNodeServerTypeForCloud(_, _, _) >> {
            null
        }
        provisionProvider.saveAndGetMorpheusServer(_, _) >> { ComputeServer serverArg, boolean fullReload ->
            // Return the same server but with a simulated "saved" state
            // This prevents the actual saving operation while maintaining the test flow
            return serverArg
        }

        // Add this to your test setup or directly in your test case
        provisionProvider.getScvmmContainerOpts(_) >> { Workload container ->
            return [
                    config: [serverConfig: 'value'],
                    vmId: 'vm-123',
                    name: 'vm-123',
                    server: container.server,
                    serverId: container.server?.id,
                    memory: 4294967296, // Using the values that would come from your test container's plan
                    maxCpu: 1,
                    maxCores: 2,
                    serverFolder: 'morpheus\\morpheus_server_1',
                    hostname: 'testVM',
                    network: [id: 1, name: 'vlanbaseVmNetwork'],
                    networkId: 1,
                    platform: 'linux',
                    externalId: 'vm-123',
                    networkType: 'vlan',
                    containerConfig: [scvmmCapabilityProfile: 'Hyper-V'],
                    resourcePool: 'Resource Pool 1',
                    hostId: 2,
                    osDiskSize: 42949672960,
                    maxTotalStorage: 71940702208,
                    dataDisks: [
                            new StorageVolume(
                                    displayOrder: 2,
                                    name: "data-2",
                                    rootVolume: false,
                                    id: 509,
                                    resizeable: true,
                                    shortName: "data-2",
                                    maxStorage: 9663676416
                            )
                    ],
                    scvmmCapabilityProfile: 'Hyper-V',
                    accountId: 1
            ]
        }

        provisionProvider.constructCloudInitOptions(_, _, _, _, _, _, _, _) >> { args ->
            Workload container = args[0]
            WorkloadRequest workloadReq = args[1]
            boolean installAgent = args[2]
            String platform = args[3]
            VirtualImage virtualImg = args[4]
            def networkConfig = args[5]
            def licenses = args[6]
            def scvmOpts = args[7]

            return [
                    cloudConfigUser: '''
            #cloud-config
            users:
              - name: morpheus
                sudo: ALL=(ALL) NOPASSWD:ALL
                shell: /bin/bash
                ssh-authorized-keys:
                  - ssh-rsa AAAAB3Nz...
        ''',
                    cloudConfigMeta: '''
            instance-id: i-test123
            local-hostname: testVM
        ''',
                    cloudConfigBytes: Base64.encoder.encodeToString('test data'.bytes),
                    cloudConfigNetwork: '''
            version: 1
            config:
              - type: physical
                name: eth0
                subnets:
                  - type: dhcp
        ''',
                    licenseApplied: true,
                    unattendCustomized: args[3]?.toLowerCase() == 'windows'
            ]
        }

        computeServerService.get(_) >> {
            computerServer
        }
        computeServerService.find(_) >> {
            controllerServer
        }
        workloadTypeService.get(_) >> {
            workloadType
        }

        storageVolumeService.save(_ as StorageVolume) >> {
            StorageVolume volume -> return volume
        }

        virtualImageService.get(_) >> {
            return virtualImage
        }

        asyncVirtualImageService.getVirtualImageFiles(_ as VirtualImage) >> {
            return mockFilesSingle
        }

        morpheusContext.getServices() >> Mock(MorpheusServices) {
            getComputeServer() >> computeServerService
            getWorkloadType() >> workloadTypeService
            getStorageVolume() >> storageVolumeService
            getVirtualImage() >> virtualImageService
        }


        morpheusContext.getAsync() >> Mock(MorpheusAsyncServices) {
            getComputeServer() >> computeServerService
            getWorkloadType() >> workloadTypeService
            getStorageVolume() >> storageVolumeService
            getVirtualImage() >> asyncVirtualImageService
        }

        // Mock the apiService methods directly with concrete return values
        //mockApiService.pickScvmmController(_) >> controllerServer

        // Using the direct object return instead of a closure
        mockApiService.getScvmmZoneOpts(_, _) >> {
            scvmmOpts
        }
        mockApiService.getScvmmControllerOpts(_, _) >> {
            mockedControllerOpts
        }

        mockApiService.createServer(_) >> { Map servOpts ->
            return [
                    success: true,
                    server: [
                            id: 'vm-12345',
                            ipAddress: '10.0.1.100',
                            name: 'testVM',
                            status: 'Running'
                    ],
                    deleteDvdOnComplete: [
                            removeIsoFromDvd: true,
                            deleteIso: false
                    ],
                    cloudConfig: [
                            isoAttached: true,
                            dvdDriveId: 'dvd-123'
                    ],
                    errors: [],
                    msg: 'Server created successfully'
            ]
        }

        mockApiService.checkServerReady(_, _) >> { Map options, String serverId ->
            return [
                    success: true,
                    server: [
                            id: serverId ?: 'vm-12345',
                            ipAddress: '10.0.1.100',
                            name: 'testVM',
                            status: 'Running',
                            VMId: 'VMm-123456'
                    ],
                    hasIp: true,
                    ready: true,
                    waitComplete: true,
                    msg: 'Server is ready'
            ]
        }

        mockApiService.setCdrom(_) >> { Map setCdromOpts ->
            return [
                    success: true,
                    data: [
                            dvdRemoved: true
                    ],
                    msg: 'CD-ROM settings updated successfully'
            ]
        }

        mockApiService.resolveConfigDatastore(_, _) >> datastore
        mockApiService.resolveControllerNode(_, _) >> node
        mockApiService.resolveHostNode(_, _, _) >> node
        mockApiService.generateDiskSpec(_, _, _, _, _) >> diskSpec
        mockApiService.provisionServer(_, _, _, _) >> new ServiceResponse<Map>(success: true, data: provisionResponse)

        when:
        def response = provisionProvider.runWorkload(workload, workloadRequest, opts)

        then:
        response.success
        response.data.success
        response.data.installAgent
        !response.data.noAgent
        response.data.externalId == "vm-12345"
        response.data.publicIp == "10.0.1.100"
        response.data.privateIp == "192.168.1.100"
    }

}
