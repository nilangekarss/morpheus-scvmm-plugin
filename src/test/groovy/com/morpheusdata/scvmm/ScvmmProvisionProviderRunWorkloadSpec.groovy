package com.morpheusdata.scvmm

import com.bertramlabs.plugins.karman.CloudFile
import com.morpheusdata.core.MorpheusAsyncServices
import com.morpheusdata.core.MorpheusComputeTypeSetService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.MorpheusProcessService
import com.morpheusdata.core.MorpheusServices
import com.morpheusdata.core.MorpheusStorageVolumeTypeService
import com.morpheusdata.core.MorpheusVirtualImageService
import com.morpheusdata.core.MorpheusComputeServerService
import com.morpheusdata.core.MorpheusStorageVolumeService
import com.morpheusdata.core.cloud.MorpheusCloudService
import com.morpheusdata.core.library.MorpheusWorkloadTypeService
import com.morpheusdata.core.synchronous.MorpheusSynchronousVirtualImageService
import com.morpheusdata.core.synchronous.cloud.MorpheusSynchronousDatastoreService
import com.morpheusdata.core.synchronous.cloud.MorpheusSynchronousCloudService
import com.morpheusdata.core.synchronous.compute.MorpheusSynchronousComputeServerService
import com.morpheusdata.core.synchronous.MorpheusSynchronousStorageVolumeService
import com.morpheusdata.core.synchronous.MorpheusSynchronousResourcePermissionService
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.network.MorpheusNetworkService
import com.morpheusdata.core.synchronous.library.MorpheusSynchronousWorkloadTypeService
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.model.*
import com.morpheusdata.model.provisioning.HostRequest
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
import com.morpheusdata.response.InitializeHypervisorResponse
import com.morpheusdata.response.PrepareWorkloadResponse
import com.morpheusdata.response.ProvisionResponse
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonOutput
import io.reactivex.rxjava3.core.Maybe
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import org.junit.jupiter.api.BeforeEach
import spock.lang.Specification
import spock.lang.Unroll

class ScvmmProvisionProviderRunWorkloadSpec extends Specification {
    private MorpheusContext morpheusContext
    private ScvmmPlugin plugin
    private ScvmmProvisionProvider provisionProvider
    private ScvmmApiService mockApiService
    private MorpheusSynchronousComputeServerService computeServerService
    private MorpheusComputeServerService asyncComputeServerService
    private MorpheusComputeTypeSetService asyncComputeTypeSetService
    private MorpheusProcessService processService
    private MorpheusSynchronousWorkloadTypeService workloadTypeService
    private MorpheusWorkloadTypeService asyncWorkloadTypeService
    private MorpheusCloudService asyncCloudService
    private MorpheusSynchronousCloudService cloudService
    private MorpheusSynchronousStorageVolumeService storageVolumeService
    private MorpheusSynchronousResourcePermissionService resourcePermissionService
    private MorpheusStorageVolumeService asyncStorageVolumeService
    private MorpheusSynchronousVirtualImageService virtualImageService
    private MorpheusVirtualImageService asyncVirtualImageService
    //private ComputeServer mockedComputerServer
    //private WorkloadType mockedWorkloadType
    @BeforeEach
    void setup() {
        // Setup mock context and services
        morpheusContext = Mock(MorpheusContext)
        plugin = Mock(ScvmmPlugin)

        // Mock services
        computeServerService = Mock(MorpheusSynchronousComputeServerService)
        asyncComputeServerService = Mock(MorpheusComputeServerService)
        asyncComputeTypeSetService = Mock(MorpheusComputeTypeSetService)
        processService = Mock(MorpheusProcessService)
        asyncCloudService = Mock(MorpheusCloudService)
        def networkService = Mock(MorpheusNetworkService)
        workloadTypeService = Mock(MorpheusSynchronousWorkloadTypeService)
        asyncWorkloadTypeService = Mock(MorpheusWorkloadTypeService)
        storageVolumeService = Mock(MorpheusSynchronousStorageVolumeService)
        resourcePermissionService = Mock(MorpheusSynchronousResourcePermissionService)
        cloudService = Mock(MorpheusSynchronousCloudService)
        asyncStorageVolumeService = Mock(MorpheusStorageVolumeService)
        virtualImageService = Mock(MorpheusSynchronousVirtualImageService)
        asyncVirtualImageService = Mock(MorpheusVirtualImageService)

        def morpheusServices = Mock(MorpheusServices) {
            getComputeServer() >> computeServerService
            getCloud() >> cloudService
            getWorkloadType() >> workloadTypeService
            getStorageVolume() >> storageVolumeService
            getVirtualImage() >> virtualImageService
            getResourcePermission() >> resourcePermissionService
        }
        def morpheusAsyncServices = Mock(MorpheusAsyncServices) {
            getCloud() >> asyncCloudService
            getNetwork() >> networkService
            getComputeServer() >> asyncComputeServerService
            getStorageVolume() >> asyncStorageVolumeService
            getVirtualImage() >> asyncVirtualImageService
            getComputeTypeSet() >> asyncComputeTypeSetService
            getWorkloadType() >> asyncWorkloadTypeService
        }

        // Configure context mocks
        morpheusContext.getProcess() >> processService
        morpheusContext.getAsync() >> morpheusAsyncServices
        morpheusContext.getServices() >> morpheusServices


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
        // Add this to your test setup
        def nodeServer = new ComputeServer(
                id: 2L,
                name: "node-01",
                externalId: "host-123",
                sshHost: "10.0.0.10",
                serverType: new ComputeServerType(code: "hypervisor"),
                hostname: "scvmm-host-01"
        )

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

        mockApiService.getServerDetails(_, _) >> { Map options, String serverId ->
            return [
                    success: true,
                    server: [
                            id: serverId ?: 'vm-12345',
                            name: 'testVM',
                            status: 'Running',
                            VMId: 'VMm-123456',
                            ipAddress: '10.0.1.100',
                            osType: 'Windows',
                            generation: 'Generation 2',
                            description: 'Test virtual machine',
                            diskSizeMB: 40960,
                            memoryMB: 4096,
                            cpuCount: 2,
                            hostName: 'scvmm-host-01',
                            hostId: 'host-123',
                            datastoreName: 'datastore1',
                            datastoreId: 'ds-123',
                            diskLayout: [
                                    systemDisk: '/dev/sda',
                                    dataDisk: '/dev/sda'
                            ]
                    ],
                    msg: 'Server details retrieved successfully'
            ]
        }
//        computeServerService.get(_) >> {
//            computerServer
//        }

        provisionProvider.loadDatastoreForVolume(_, _, _, _) >> { Cloud cld, String hostVolumeId, String fileShareId, String partitionId ->
            return datastore // Return the datastore object you created in your test setup
        }

        // Mock applyComputeServerNetworkIp
        provisionProvider.applyComputeServerNetworkIp(_, _, _, _, _) >> { ComputeServer serverObj, String internalIp, String externalIp, int index, def macAddress  ->
            return new ComputeServerInterface(
                    id: 1L,
                    ipAddress: internalIp,
                    publicIpAddress: externalIp,
                    externalId: 'nic-123',
                    primaryInterface: true,
                    network: new Network(id: 1L, name: 'vlanbaseVmNetwork')
            )
        }

        provisionProvider.cloneParentCleanup(_, _) >> { Map cloneParentCleanOpts, ServiceResponse response ->
            // You can add verification logic here if needed
            return null // Or return whatever the method should return
        }

        // Then modify your existing mock for computeServerService.get() to handle different IDs
        computeServerService.get(_) >> { Long id ->
            if (id == nodeServer.id) {
                return nodeServer
            } else if (id == computerServer.id) {
                return computerServer
            } else {
                return controllerServer
            }
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

        asyncComputeServerService.save(_) >> { ComputeServer serverObj ->
            return Single.just(serverObj)
        }

        // Mock process.startProcessStep

        processService.startProcessStep(_, _, _) >> {
            return Single.just(true)
        }

        morpheusContext.getProcess() >> processService

        morpheusContext.getServices() >> Mock(MorpheusServices) {
            getComputeServer() >> computeServerService
            getWorkloadType() >> workloadTypeService
            getStorageVolume() >> storageVolumeService
            getVirtualImage() >> virtualImageService

        }



        morpheusContext.getAsync() >> Mock(MorpheusAsyncServices) {
            getComputeServer() >> asyncComputeServerService
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
                            status: 'Running',
                            MId: 'VMm-123456',
                            disks: [
                                    osDisk: [
                                            externalId: 'os-disk-id-1'
                                    ],
                                    dataDisks: [
                                            [
                                                    id: 509,  // This should match the ID of your non-root volume
                                                    externalId: 'data-disk-id-1'
                                            ]
                                    ],
                                    diskMetaData: [
                                            'os-disk-id-1': [
                                                    VhdID: 'vhd-os-123',
                                                    HostVolumeId: 'hvol-1',
                                                    FileShareId: 'fs-1',
                                                    PartitionUniqueId: 'part-1'
                                            ],
                                            'data-disk-id-1': [
                                                    VhdID: 'vhd-data-123',
                                                    HostVolumeId: 'hvol-2',
                                                    FileShareId: 'fs-2',
                                                    PartitionUniqueId: 'part-2'
                                            ],
                                            'vhd-os-123': [
                                                    HostVolumeId: 'hvol-1',
                                                    FileShareId: 'fs-1',
                                                    PartitionUniqueId: 'part-1'
                                            ],
                                            'vhd-data-123': [
                                                    HostVolumeId: 'hvol-2',
                                                    FileShareId: 'fs-2',
                                                    PartitionUniqueId: 'part-2'
                                            ]
                                    ]
                            ]
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


        when:
        def response = provisionProvider.runWorkload(workload, workloadRequest, opts)

        then:
        response.success
        response.data.success

    }

    def "getProvisionTypeCode should return scvmm"() {
        expect:
        provisionProvider.getProvisionTypeCode() == "scvmm"
    }

    def "getCircularIcon should return an Icon object with provision-circular.svg path"() {
        when:
        def result = provisionProvider.getCircularIcon()

        then:
        result instanceof com.morpheusdata.model.Icon
        result.path == "provision-circular.svg"
        result.darkPath == "provision-circular-dark.svg"
    }

    def "getOptionTypes should return non-empty list"() {
        when:
        def result = provisionProvider.getOptionTypes()

        then:
        result != null
        !result.isEmpty()
        result.every { it instanceof OptionType }
    }

    def "getNodeOptionTypes should return non-empty list"() {
        when:
        def result = provisionProvider.getNodeOptionTypes()

        then:
        result != null
        !result.isEmpty()
        result.every { it instanceof OptionType }
    }

    def "getRootVolumeStorageTypes should return non-empty list"() {
        given:
        def mockStorageTypes = [new StorageVolumeType(code: 'standard')]
        def mockedStorageVolTypeService =  Mock(MorpheusStorageVolumeTypeService)
        asyncStorageVolumeService.getStorageVolumeType() >> mockedStorageVolTypeService
        mockedStorageVolTypeService.list(_) >> Observable.fromIterable(mockStorageTypes)
        //storageVolumeService.listStorageVolumeTypes() >> mockStorageTypes

        when:
        def result = provisionProvider.getRootVolumeStorageTypes()

        then:
        result != null
        !result.isEmpty()
        result == mockStorageTypes
    }

    def "getDataVolumeStorageTypes should return non-empty list"() {
        given:
        def mockStorageTypes = [new StorageVolumeType(code: 'standard')]
        def mockedStorageVolTypeService =  Mock(MorpheusStorageVolumeTypeService)
        asyncStorageVolumeService.getStorageVolumeType() >> mockedStorageVolTypeService
        mockedStorageVolTypeService.list(_) >> Observable.fromIterable(mockStorageTypes)


        when:
        def result = provisionProvider.getDataVolumeStorageTypes()

        then:
        result != null
        !result.isEmpty()
        result == mockStorageTypes
    }

    def "getServicePlans should return non-empty list"() {
        when:
        def result = provisionProvider.getServicePlans()

        then:
        result != null
        !result.isEmpty()
    }

    def "validateWorkload should return success response"() {
        given:
        def opts = [:]

        when:
        def response = provisionProvider.validateWorkload(opts)

        then:
        response.success
    }

    def "getCode should return scvmm"() {
        expect:
        provisionProvider.getCode() == "scvmm.provision"
    }

    def "getName should return 'SCVMM'"() {
        expect:
        provisionProvider.getName() == "SCVMM"
    }

    def "getMorpheus should return the morpheus context"() {
        expect:
        provisionProvider.getMorpheus() == morpheusContext
    }

    def "getPlugin should return the plugin"() {
        expect:
        provisionProvider.getPlugin() == plugin
    }

    @Unroll
    def "#methodName should return #expectedValue"() {
        when:
        def result = provisionProvider."$methodName"()

        then:
        if (methodName == "getHostType") {
            result.toString() == "vm" // Compare string representation for HostType
        } else {
            result == expectedValue
        }

        where:
        methodName                   | expectedValue
        "hasNetworks"                | true
        "getMaxNetworks"             | 1
        "canAddVolumes"              | true
        "canCustomizeRootVolume"     | true
        "canResizeRootVolume"        | true
        "canCustomizeDataVolumes"    | true
        "hasDatastores"              | true
        "getHostType"                | "vm"
        "serverType"                 | "vm"
        "supportsCustomServicePlans" | true
        "multiTenant"                | false
        "aclEnabled"                 | false
        "customSupported"            | true
        "lvmSupported"               | true
        "getNodeFormat"              | "vm"
        "hasSecurityGroups"          | false
        "hasNodeTypes"               | true
        "getHostDiskMode"            | "lvm"
        "hasComputeZonePools"        | true
    }

    def "getDeployTargetService should return vmDeployTargetService"() {
        expect:
        provisionProvider.getDeployTargetService() == "vmDeployTargetService"
    }

    def "createDefaultInstanceType should return false"() {
        expect:
        provisionProvider.createDefaultInstanceType() == false
    }

    @Unroll
    def "validateHost should #scenario"() {
        given:
        def server = Mock(ComputeServer) {
            getComputeServerType() >> Mock(ComputeServerType) {
                isVmHypervisor() >> isVmHypervisor
            }
        }

        when:
        def response = provisionProvider.validateHost(server, opts)

        then:
        if (apiCallExpected) {
            1 * mockApiService.validateServerConfig(expectedApiArgs) >> apiResponse
        }
        response.success == expectedSuccess
        if (errorCheck) {
            response.errors == validationErrors
        }

        where:
        scenario                                    | isVmHypervisor | opts                                              | apiCallExpected | expectedApiArgs                                                         | apiResponse                                                         | expectedSuccess | errorCheck | validationErrors
        "return success when server is a VM hypervisor" | true           | [:]                                               | false           | null                                                                   | null                                                                | true           | false      | null
        "validate server config when not VM hypervisor"  | false          | [networkInterfaces:[[network:[id:123L]]], config:[scvmmCapabilityProfile:"Hyper-V", nodeCount:1]] | true            | [networkId:123L, scvmmCapabilityProfile:"Hyper-V", nodeCount:1]        | ServiceResponse.success()                                           | true           | false      | null
        "handle network ID from interface field"         | false          | [networkInterface:[network:[id:123L]]]           | true            | [networkId:123L, scvmmCapabilityProfile:null, nodeCount:null]           | ServiceResponse.success()                                           | true           | false      | null
        "handle network ID from config field"            | false          | [config:[networkInterface:[network:[id:123L]]]]  | true            | [networkId:123L, scvmmCapabilityProfile:null, nodeCount:null]           | ServiceResponse.success()                                           | true           | false      | null
        "handle network ID from interfaces array"        | false          | [networkInterfaces:[[network:[id:123L]]]]        | true            | [networkId:123L, scvmmCapabilityProfile:null, nodeCount:null]           | ServiceResponse.success()                                           | true           | false      | null
        "return error when validation fails"             | false          | [networkInterfaces:[[network:[id:123L]]]]        | true            | [networkId:123L, scvmmCapabilityProfile:null, nodeCount:null]           | new ServiceResponse(success:false, errors:[error:"Invalid configuration"]) | false          | true       | [error:"Invalid configuration"]
        "handle exceptions gracefully"                   | false          | [networkInterfaces:[[network:[id:123L]]]]        | true            | [networkId:123L, scvmmCapabilityProfile:null, nodeCount:null]           | { throw new RuntimeException("Test exception") }                    | true           | false      | null
    }

    @Unroll
    def "getVolumePathForDatastore should return #expectedPath for datastore #datastoreDescription"() {
        given:

        def datastore = datastoreInput ? new Datastore(id: 123L, name: "test-datastore") : null

        // Mock the storage volume response
        if (hasStorageVolume) {
            def storageVolume = new StorageVolume(volumePath: expectedPath)
            storageVolumeService.find({ DataQuery query ->
                query.filters.any { it.name == 'datastore.id' && it.value == datastore?.id } &&
                        query.filters.any { it.name == 'volumePath' && it.operator == '!=' && it.value == null }
            }) >> {
             return storageVolume
            }
        } else {
            storageVolumeService.find(_) >> null
        }

        when:
        def result = provisionProvider.getVolumePathForDatastore(datastore)

        then:
        result == expectedPath

        where:
        datastoreDescription     | datastoreInput | hasStorageVolume | expectedPath
        "valid datastore"        | true           | true             | "\\\\server\\path\\to\\volume"
        "datastore with no path" | true           | false            | null
        "null datastore"         | false          | false            | null
    }

    @Unroll
    def "testHostAndDatastore #hostAndDataScenario"() {
        given:
        // Setup cloud and account
        def cloud = new Cloud(id: 1L, name: 'test-cloud', regionCode: hasCloudRegion ? 'region1' : null)
        def account = new Account(id: 2L, name: 'test-account')

        // Setup datastores
        def datastore1 = new Datastore(id: 10L, name: 'datastore1', freeSpace: 100000000000L)
        def datastore2 = new Datastore(id: 11L, name: 'datastore2', freeSpace: 200000000000L)
        if (isSharedVolume) {
            datastore1.zonePool = new ComputeZonePool(id: 5L, name: 'zone-pool-1')
        }

        // Convert datastore parameter string to actual datastore object
        def datastoreObj = datastoreParam == 'datastore1' ? datastore1 :
                (datastoreParam == 'datastore2' ? datastore2 : null)

        // Setup computeServer/node
        def node1 = new ComputeServer(
                id: 20L,
                name: 'node1',
                enabled: true,
                cloud: cloud,
                computeServerType: new ComputeServerType(code: 'scvmmHypervisor'),
                powerState: ComputeServer.PowerState.on,
                capacityInfo: new ComputeCapacityInfo(maxMemory: 8589934592L, usedMemory: 2147483648L)
        )
        def node2 = new ComputeServer(
                id: 21L,
                name: 'node2',
                enabled: true,
                cloud: cloud,
                computeServerType: new ComputeServerType(code: 'scvmmHypervisor'),
                powerState: ComputeServer.PowerState.on,
                capacityInfo: new ComputeCapacityInfo(maxMemory: 17179869184L, usedMemory: 4294967296L)
        )

        // Setup storage volumes for the nodes
        def volume1 = new StorageVolume(id: 30L, datastore: datastore1)
        def volume2 = new StorageVolume(id: 31L, datastore: datastore2)
        node1.volumes = [volume1]
        node2.volumes = [volume1, volume2]

        resourcePermissionService.listAccessibleResources(_, _, _, _) >> [datastore1.id, datastore2.id]

        // Mock cloud datastore service
        def datastoreService = Mock(MorpheusSynchronousDatastoreService)
        cloudService.getDatastore() >> datastoreService

        // Mock getVolumePathForDatastore
        if (mockVolumePathDatastore) {
            def dsForPath = mockVolumePathDatastore == 'datastore1' ? datastore1 : datastore2
            provisionProvider.getVolumePathForDatastore(dsForPath) >> "\\\\server\\path\\to\\volume"
           // provisionProvider.getVolumePathForDatastore(mockVolumePathDatastore) >> "\\\\server\\path\\to\\volume"
        }

        // Configure mocks based on test scenario
        if (hostIdParam) {
            computeServerService.get(hostId) >> node1
        }

        if (scopedDatastoreIds) {
            computeServerService.list({ DataQuery query ->
                query.filters.any { it.name == 'hostId' && it.value == hostId }
            }) >> [node1]
        }

        if (searchForDatastores) {

            // Create proper list of datastore objects instead of using string
            def datastoreList = []
            if (datastoreResults == '[datastore1]') {
                datastoreList = [datastore1]
            }
            datastoreService.list(_ as DataQuery) >> datastoreList
        }

        if (searchForNodes) {
            computeServerService.list({ DataQuery query ->
                // For datastore_provided_directly scenario, return node1 instead of node2
                if (datastoreParam == 'datastore1') {
                    return [node1]  // Return node1 with ID 20 to match the expected result
                } else if (datastoreParam == 'datastore2') {
                    return [node2]
                }
                return []
            }) >> { DataQuery query ->
                if (datastoreParam == 'datastore1') {
                    return [node1]  // Return node1 with ID 20 to match the expected result
                } else if (datastoreParam == 'datastore2') {
                    return [node2]
                }
                return []
            }
        }

        when:
        def result = provisionProvider.getHostAndDatastore(cloud, account,
                clusterIdParam ? clusterIdParam : null,
                hostIdParam ? hostIdParam : null,
                datastoreObj,
                datastoreOption,
                size,
                siteId,
                requiredMemory)

        then:
        result[0]?.id == expectedNodeId
        result[1]?.id == expectedDatastoreId
        result[2] == expectedVolumePath
        result[3] == expectedHighlyAvailable

        where:
        hostAndDataScenario                | hasCloudRegion | clusterIdParam | hostIdParam | hostId | datastoreParam | datastoreOption | size          | siteId | requiredMemory | isSharedVolume | scopedDatastoreIds | searchForDatastores | datastoreResults | searchForNodes | mockVolumePathDatastore | expectedNodeId | expectedDatastoreId | expectedVolumePath            | expectedHighlyAvailable
        "cloud_deployment_with_auto_ds"    | true           | null           | null        | null   | null           | 'auto'          | 10000000000L  | null   | 4294967296L    | false          | false              | false               | []               | false          | null                    | null           | null               | null                          | false
        "host_specified_by_user"           | true           | null           | '20'        | 20L    | null           | 'manual'        | 10000000000L  | null   | 4294967296L    | false          | true               | true                | '[datastore1]'   | false          | 'datastore1'            | 20L            | 10L                | "\\\\server\\path\\to\\volume" | false
        "datastore_provided_directly"      | true           | null           | null        | null   | 'datastore1'   | 'manual'        | 10000000000L  | null   | 4294967296L    | false          | false              | false               | []               | true           | 'datastore1'            | 20L            | 10L                | "\\\\server\\path\\to\\volume" | false
        "cluster_with_shared_volume"       | true           | '5'            | null        | null   | 'datastore1'   | 'manual'        | 10000000000L  | null   | 4294967296L    | true           | false              | false               | []               | true           | 'datastore1'            | 20L            | 10L                | "\\\\server\\path\\to\\volume" | true
        "non_cloud_with_node_and_ds"       | false          | null           | '20'        | 20L    | null           | 'manual'        | 10000000000L  | null   | 4294967296L    | false          | true               | true                | '[datastore1]'   | false          | 'datastore1'            | 20L            | 10L                | "\\\\server\\path\\to\\volume" | false
        "ds_with_insufficient_mem_nodes"   | true           | null           | null        | null   | 'datastore1'   | 'manual'        | 10000000000L  | null   | 10737418240L   | false          | false              | false               | []               | true           | 'datastore1'            | null           | 10L                | "\\\\server\\path\\to\\volume" | false
    }


    @Unroll
    def "test_getHostAndDatastore_exceptionCases"() {
        given:
        // Setup cloud and account
        def cloud = new Cloud(id: 1L, name: 'test-cloud', regionCode: hasCloudRegion ? 'region1' : null)
        def account = new Account(id: 2L, name: 'test-account')

        // Setup datastores - empty for this test case
        resourcePermissionService.listAccessibleResources(_, _, _, _) >> []

        // Mock cloud datastore service
        def datastoreService = Mock(MorpheusSynchronousDatastoreService)
        cloudService.getDatastore() >> datastoreService
        datastoreService.list(_ as DataQuery) >> []

        when:
        provisionProvider.getHostAndDatastore(cloud, account, clusterIdParam, hostIdParam,
                datastoreParam, datastoreOption, size, siteId, requiredMemory)


        then:
        thrown(Exception)

        where:
        scenario                               | hasCloudRegion | clusterIdParam | hostIdParam | hostId | datastoreParam | datastoreOption | size          | siteId | requiredMemory
        "non_cloud_without_node_and_datastore" | false          | null           | null        | null   | null           | 'manual'        | 10000000000L  | null   | 4294967296L
    }


    def "test prepareWorkload returns successful response with workload"() {
        given:
        def provider = new ScvmmProvisionProvider(null, null)
        def workload = Mock(Workload) {
            getName() >> "test-workload"
            getServer() >> Mock(ComputeServer) {
                getName() >> "test-server"
            }
        }
        def workloadRequest = Mock(WorkloadRequest)
        def opts = [key: "value"]

        when:
        def response = provider.prepareWorkload(workload, workloadRequest, opts)

        then:
        response.success
        response.msg == ''
        response.errors == null
        response.data instanceof PrepareWorkloadResponse
        response.data.workload == workload
    }

    def "test initializeHypervisor with shared controller"() {
        given:
        ComputeServer server = new ComputeServer(id: 1)
        Cloud cloud = new Cloud(id: 1, code: 'scvmm', configMap: [sharedController: "123"])

        when:
        def response = provisionProvider.initializeHypervisor(cloud, server)

        then:
        0 * mockApiService._
        response.success
        response.data instanceof InitializeHypervisorResponse
    }

    def "test initializeHypervisor with successful server info retrieval"() {
        given:
        ComputeServer server = new ComputeServer(id: 1)
        Cloud cloud = new Cloud(id: 1, code: 'scvmm', configMap: [:])
        Map zoneOpts = [hostName: 'scvmmserver', username: 'admin', password: 'password']
        Map controllerOpts = [controllerHostname: 'scvmmserver']
        Map serverInfo = [
                success: true,
                hostname: 'scvmmserver.local',
                disks: 500 * ComputeUtility.ONE_GIGABYTE,
                memory: 16 * ComputeUtility.ONE_GIGABYTE,
                osName: 'Microsoft Windows Server 2019 Datacenter'
        ]

        when:
        def response = provisionProvider.initializeHypervisor(cloud, server)

        then:
        1 * mockApiService.getScvmmZoneOpts(morpheusContext, cloud) >> zoneOpts
        1 * mockApiService.getScvmmControllerOpts(cloud, server) >> controllerOpts
        1 * mockApiService.getScvmmServerInfo(zoneOpts + controllerOpts) >> serverInfo
        1 * mockApiService.extractWindowsServerVersion(serverInfo.osName) >> 'windows.server.2019'
        1 * mockApiService.prepareNode(zoneOpts + controllerOpts)
        response.success
        response.data.commType.toString() == 'winrm'
        response.data.maxMemory == serverInfo.memory
        response.data.maxStorage == serverInfo.disks
        response.data.maxCores == 1
        response.data.serverOs.code == 'windows.server.2019'
        server.hostname == 'scvmmserver.local'
    }

    def "test initializeHypervisor handles exception"() {
        given:
        ComputeServer server = new ComputeServer(id: 1)
        Cloud cloud = new Cloud(id: 1, code: 'scvmm', configMap: [:])

        when:
        def response = provisionProvider.initializeHypervisor(cloud, server)

        then:
        1 * mockApiService.getScvmmZoneOpts(morpheusContext, cloud) >> { throw new RuntimeException("API error") }
        0 * mockApiService.prepareNode(_)
        response.data instanceof InitializeHypervisorResponse
        !response.success
    }

    @Unroll
    def "waitForHost returns success when server is ready and finalize succeeds"() {
        given:
        Cloud cloud = new Cloud(id: 1L, code: 'scvmm-cloud')

        ComputeServer server = new ComputeServer(
                id: 100L,
                cloud: cloud,
                externalId: 'vm-123',
                name: 'test-server',
                config: '{"hostId":"200"}'
        )

        ComputeServer controllerServer = new ComputeServer(
                id: 200L,
                cloud: cloud,
                computeServerType: new ComputeServerType(code: 'scvmmController')
        )

        def serverDetail = [
                success: true,
                server: [ipAddress: '10.0.0.100']
        ]

        morpheusContext.services.computeServer.get(200L) >> {
            return controllerServer
        }
        mockApiService.getScvmmCloudOpts(morpheusContext, cloud, controllerServer) >> {
            return [cloud: cloud.id]
        }
        mockApiService.getScvmmControllerOpts(cloud, controllerServer) >> {
            return [controller: controllerServer.id]
        }
        provisionProvider.getScvmmServerOpts(server) >> {
            return [server: server.id, vmId: 'vm-123']
        }
        mockApiService.checkServerReady(_, 'vm-123') >> {
            return serverDetail
        }

        // Mock finalizeHost call - this is a method in the same class, we need to spy it
        provisionProvider.metaClass.finalizeHost = { ComputeServer srv ->
            return new ServiceResponse(success: true)
        }

        asyncComputeServerService.save(_) >> { ComputeServer serverObj ->
            return Single.just(serverObj)
        }
        provisionProvider.applyComputeServerNetworkIp(_, _, _, _, _) >> {
            return new ComputeServerInterface(
                    id: 1L,
                    ipAddress: '10.0.0.5',
                    name: 'eth0',
                    primaryInterface: true,
                    publicIpAddress: '10.0.0.5',
                    macAddress: '00:11:22:33:44:55',
                    displayOrder: 1,
                    addresses: [new NetAddress(type: NetAddress.AddressType.IPV4, address: '10.0.0.5')]
            )
        }

        when:
        def response = provisionProvider.waitForHost(server)

        then:

        response.success == true
        response.data.privateIp == '10.0.0.100'
        response.data.publicIp == '10.0.0.100'
        response.data.externalId == 'vm-123'
        response.data.success == true
    }

    @Unroll
    def "finalizeHost successfully processes server when checkServerReady succeeds"() {
        given:
        Cloud cloud = new Cloud(id: 1L, code: 'scvmm-cloud')

        ComputeServer server = new ComputeServer(
                id: 100L,
                cloud: cloud,
                externalId: 'vm-123',
                name: 'test-server',
                config: '{"hostId":"200"}'
        )

        ComputeServer controllerNode = new ComputeServer(
                id: 200L,
                cloud: cloud,
                computeServerType: new ComputeServerType(code: 'scvmmController')
        )

        def serverDetail = [
                success: true,
                server: [ipAddress: '10.0.0.100']
        ]

        // Mock necessary service calls
        computeServerService.get(200L) >> controllerNode

        mockApiService.getScvmmCloudOpts(morpheusContext, server.cloud, controllerNode) >> [cloudId: 1L]
        mockApiService.getScvmmControllerOpts(server.cloud, controllerNode) >> [controllerId: 200L]

        provisionProvider.getScvmmServerOpts(server) >> [vmId: 'vm-123']

        mockApiService.checkServerReady(_, 'vm-123') >> serverDetail

        // Mock the network IP application
        provisionProvider.applyComputeServerNetworkIp(_,_,_,_,_) >> {
            return new ComputeServerInterface(
                    id: 1L,
                    ipAddress: '192.168.1.100',
                    macAddress: '00:11:22:33:44:55'
            )
        }

        // Mock the server save operation
        def serverSingle = Single.just(server)
        asyncComputeServerService.save(server) >> serverSingle

        when:
        def response = provisionProvider.finalizeHost(server)

        then:
        response.success == true
        1 * asyncComputeServerService.save(server) >> serverSingle
    }

    @Unroll
    def "prepareHost sets source image when server has a typeSet with a workloadType that has a virtualImage"() {
        given:
        def server = new ComputeServer(id: 1L, typeSet: new ComputeTypeSet(id: 123L))
        def hostRequest = new HostRequest()
        def opts = [:]
        def computeTypeSet1 = new ComputeTypeSet()
        def workloadType = new WorkloadType(id: 456L, virtualImage: new VirtualImage(id: 789L))
        def virtualImage = new VirtualImage(id: 789L)

        computeTypeSet1.workloadType = workloadType
        computeTypeSet1.id  = 123L

        asyncComputeTypeSetService.get(_) >> {
            return Maybe.just(computeTypeSet1)
        }
        asyncWorkloadTypeService.get(_) >> Maybe.just(workloadType)

        provisionProvider.saveAndGet(_) >> { ComputeServer srv ->
            // Just return the server without making changes
            return srv
        }

        when:
        def response = provisionProvider.prepareHost(server, hostRequest, opts)

        then:

        response.success
        response.data.computeServer == server
        server.sourceImage.id == 789L
    }

    def "stopServer handles different scenarios"() {
        given:
        // Create a test server with optional externalId
        def server = new ComputeServer(
                id: 100L,
                name: "test-server",
                externalId: externalId,
                cloud: new Cloud(id: 1L)
        )

        // Setup mocks based on scenario
        if (externalId) {
            def serverOpts = [
                    externalId: externalId,
                    server: server,
                    cloud: server.cloud
            ]
            provisionProvider.getAllScvmmServerOpts(server) >> serverOpts
            if (apiThrowsException) {
                mockApiService.stopServer(serverOpts, externalId) >> { throw new RuntimeException(errorMessage) }
            } else {
                mockApiService.stopServer(serverOpts, externalId) >> [success: apiSuccess]
            }
        }

        when:
        def response = provisionProvider.stopServer(server)

        then:
        response.success == expectedSuccess
        response.msg == expectedMessage
        if (!externalId) {
            0 * mockApiService.stopServer(_, _) // Verify API was not called when no externalId
        }

        where:
        scenario           | externalId | apiSuccess | apiThrowsException | errorMessage           | expectedSuccess | expectedMessage
        "successful stop"  | "vm-123"   | true       | false              | null                   | true           | null
        "no externalId"    | null       | false      | false              | null                   | false          | "vm not found"
        "api exception"    | "vm-123"   | false      | true               | "API connection error" | false          | "API connection error"
    }

    def "startServer handles different scenarios"() {
        given:
        // Create a test server with optional externalId
        def server = new ComputeServer(
                id: 100L,
                name: "test-server",
                externalId: externalId,
                cloud: new Cloud(id: 1L)
        )

        // Setup mocks based on scenario
        if (externalId) {
            def serverOpts = [
                    externalId: externalId,
                    server: server,
                    cloud: server.cloud
            ]
            provisionProvider.getAllScvmmServerOpts(server) >> serverOpts
            if (apiThrowsException) {
                mockApiService.startServer(serverOpts, externalId) >> { throw new RuntimeException(errorMessage) }
            } else {
                mockApiService.startServer(serverOpts, externalId) >> [success: apiSuccess]
            }
        }

        when:
        def response = provisionProvider.startServer(server)

        then:
        response.success == expectedSuccess
        response.msg == expectedMessage
        if (!externalId) {
            0 * mockApiService.startServer(_, _) // Verify API was not called when no externalId
        }

        where:
        scenario           | externalId | apiSuccess | apiThrowsException | errorMessage           | expectedSuccess | expectedMessage
        "successful start" | "vm-123"   | true       | false              | null                   | true           | null
        "failed start"     | "vm-123"   | false      | false              | null                   | false          | null
        "no externalId"    | null       | false      | false              | null                   | false          | "externalId not found"
        "api exception"    | "vm-123"   | false      | true               | "API connection error" | false          | null
    }

    def "getServerDetails returns response with server IP addresses"() {
        given:
        // Create test servers with different IP configurations
        def serverWithBothIps = new ComputeServer(
                id: 100L,
                name: "server-both-ips",
                internalIp: "192.168.1.100",
                externalIp: "10.0.1.100"
        )

        def serverWithOnlyInternalIp = new ComputeServer(
                id: 101L,
                name: "server-internal-only",
                internalIp: "192.168.1.101",
                externalIp: null
        )

        def serverWithOnlyExternalIp = new ComputeServer(
                id: 102L,
                name: "server-external-only",
                internalIp: null,
                externalIp: "10.0.1.102"
        )

        def serverWithNoIps = new ComputeServer(
                id: 103L,
                name: "server-no-ips",
                internalIp: null,
                externalIp: null
        )

        when:
        def responseBothIps = provisionProvider.getServerDetails(serverWithBothIps)
        def responseInternalOnly = provisionProvider.getServerDetails(serverWithOnlyInternalIp)
        def responseExternalOnly = provisionProvider.getServerDetails(serverWithOnlyExternalIp)
        def responseNoIps = provisionProvider.getServerDetails(serverWithNoIps)

        then:
        // Test server with both IPs
        responseBothIps.success == true
        responseBothIps.data.success == true
        responseBothIps.data.privateIp == "192.168.1.100"
        responseBothIps.data.publicIp == "10.0.1.100"

        // Test server with only internal IP
        responseInternalOnly.success == true
        responseInternalOnly.data.success == true
        responseInternalOnly.data.privateIp == "192.168.1.101"
        responseInternalOnly.data.publicIp == null

        // Test server with only external IP
        responseExternalOnly.success == true
        responseExternalOnly.data.success == true
        responseExternalOnly.data.privateIp == null
        responseExternalOnly.data.publicIp == "10.0.1.102"

        // Test server with no IPs
        responseNoIps.success == true
        responseNoIps.data.success == true
        responseNoIps.data.privateIp == null
        responseNoIps.data.publicIp == null
    }
}
