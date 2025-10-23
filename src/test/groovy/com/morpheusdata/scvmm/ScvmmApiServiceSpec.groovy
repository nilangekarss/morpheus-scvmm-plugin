package com.morpheusdata.scvmm

import com.morpheusdata.core.MorpheusAsyncServices
import com.morpheusdata.core.MorpheusComputeServerService
import com.morpheusdata.core.MorpheusComputeTypeSetService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.MorpheusProcessService
import com.morpheusdata.core.MorpheusServices
import com.bertramlabs.plugins.karman.CloudFile
import com.morpheusdata.core.MorpheusStorageVolumeService
import com.morpheusdata.core.MorpheusVirtualImageService
import com.morpheusdata.core.cloud.MorpheusCloudService
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.library.MorpheusWorkloadTypeService
import com.morpheusdata.core.network.MorpheusNetworkService
import com.morpheusdata.core.synchronous.MorpheusSynchronousResourcePermissionService
import com.morpheusdata.core.synchronous.MorpheusSynchronousStorageVolumeService
import com.morpheusdata.core.synchronous.MorpheusSynchronousVirtualImageService
import com.morpheusdata.core.synchronous.cloud.MorpheusSynchronousCloudService
import com.morpheusdata.core.synchronous.library.MorpheusSynchronousWorkloadTypeService
import com.morpheusdata.core.synchronous.network.MorpheusSynchronousNetworkService
import com.morpheusdata.core.synchronous.MorpheusSynchronousKeyPairService
import com.morpheusdata.model.Account
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeZoneRegion
import com.morpheusdata.model.KeyPair
import com.morpheusdata.model.Network
import com.morpheusdata.model.StorageVolume
import org.junit.jupiter.api.BeforeEach
import spock.lang.Specification
import io.reactivex.rxjava3.core.Single
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.core.synchronous.compute.MorpheusSynchronousComputeServerService
import com.morpheusdata.core.synchronous.MorpheusSynchronousFileCopyService
import spock.lang.Unroll

class ScvmmApiServiceSpec extends Specification {

    private MorpheusContext morpheusContext
    private ScvmmPlugin plugin
    private ScvmmApiService apiService
    private MorpheusSynchronousComputeServerService computeServerService
    private MorpheusComputeServerService asyncComputeServerService
    private MorpheusComputeTypeSetService asyncComputeTypeSetService
    private MorpheusProcessService processService
    private MorpheusSynchronousWorkloadTypeService workloadTypeService
    private MorpheusWorkloadTypeService asyncWorkloadTypeService
    private MorpheusCloudService asyncCloudService
    private MorpheusSynchronousCloudService cloudService
    private MorpheusSynchronousNetworkService networkService
    private MorpheusSynchronousStorageVolumeService storageVolumeService
    private MorpheusSynchronousResourcePermissionService resourcePermissionService
    private MorpheusStorageVolumeService asyncStorageVolumeService
    private MorpheusSynchronousVirtualImageService virtualImageService
    private MorpheusVirtualImageService asyncVirtualImageService
    private MorpheusSynchronousFileCopyService fileCopyService

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
        def asyncNetworkService = Mock(MorpheusNetworkService)
        workloadTypeService = Mock(MorpheusSynchronousWorkloadTypeService)
        asyncWorkloadTypeService = Mock(MorpheusWorkloadTypeService)
        storageVolumeService = Mock(MorpheusSynchronousStorageVolumeService)
        resourcePermissionService = Mock(MorpheusSynchronousResourcePermissionService)
        cloudService = Mock(MorpheusSynchronousCloudService)
        networkService = Mock(MorpheusSynchronousNetworkService)
        asyncStorageVolumeService = Mock(MorpheusStorageVolumeService)
        virtualImageService = Mock(MorpheusSynchronousVirtualImageService)
        asyncVirtualImageService = Mock(MorpheusVirtualImageService)
        fileCopyService = Mock(MorpheusSynchronousFileCopyService)

        def morpheusServices = Mock(MorpheusServices) {
            getComputeServer() >> computeServerService
            getCloud() >> cloudService
            getWorkloadType() >> workloadTypeService
            getStorageVolume() >> storageVolumeService
            getVirtualImage() >> virtualImageService
            getResourcePermission() >> resourcePermissionService
            getFileCopy() >> fileCopyService
        }
        def morpheusAsyncServices = Mock(MorpheusAsyncServices) {
            getCloud() >> asyncCloudService
            getNetwork() >> asyncNetworkService
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

        //apiService = new ScvmmApiService(morpheusContext)
        apiService = Spy(ScvmmApiService, constructorArgs: [morpheusContext])
    }

    @Unroll
    def "test transferImage transfers image successfully"() {
        given:
        def mockedComputerServer = Mock(ComputeServer)
        morpheusContext.getServices() >> Mock(MorpheusServices) {
            getComputeServer() >> {
                return computeServerService
            }
        }

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

    @Unroll
    def "test snapshotServer successfully creates a checkpoint"() {
        given:
        def vmId = "vm-12345"
        def opts = [
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]

        // Mock the command execution with a successful result
        morpheusContext.executeWindowsCommand(*_) >> Single.just([
                success: true,
                exitCode: '0',
                data: '{"Status":"Success"}'
        ])

        when:
        def result = apiService.snapshotServer(opts, vmId)

        then:
        1 * morpheusContext.executeWindowsCommand(
                'scvmm-server',
                5985,
                'admin',
                'password',
                { String cmd -> cmd.contains("Get-SCVirtualMachine") && cmd.contains("New-SCVMCheckpoint") && cmd.contains(vmId) },
                null,
                false
        ) >> Single.just([success: true, exitCode: '0', data: '{"Status":"Success"}'])

        result.success == true
        result.snapshotId != null
    }

    @Unroll
    def "test deleteSnapshot successfully removes a checkpoint"() {
        given:
        def vmId = "vm-12345"
        def snapshotId = "snapshot-6789"
        def opts = [
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]

        // Mock the command execution with a successful result
        morpheusContext.executeWindowsCommand(*_) >> Single.just([
                success: true,
                exitCode: '0',
                data: '{"Status":"Success"}'
        ])

        when:
        def result = apiService.deleteSnapshot(opts, vmId, snapshotId)

        then:
        1 * morpheusContext.executeWindowsCommand(
                'scvmm-server',
                5985,
                'admin',
                'password',
                { String cmd ->
                    cmd.contains('$VM = Get-SCVirtualMachine -VMMServer localhost -ID "vm-12345"') &&
                            cmd.contains('$Checkpoint = Get-SCVMCheckpoint -VM $VM | where {$_.Name -like "snapshot-6789"}') &&
                            cmd.contains('$ignore = Remove-SCVMCheckpoint -VMCheckpoint $Checkpoint')
                },
                null,
                false
        ) >> Single.just([success: true, exitCode: '0', data: '{"Status":"Success"}'])

        result.success == true
        result.snapshotId == snapshotId
    }

    @Unroll
    def "test restoreServer successfully restores a VM to a checkpoint"() {
        given:
        def vmId = "vm-12345"
        def snapshotId = "snapshot-6789"
        def opts = [
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]

        // Mock the command execution with a successful result
        morpheusContext.executeWindowsCommand(*_) >> Single.just([
                success: true,
                exitCode: '0',
                data: '{"Status":"Success"}'
        ])

        when:
        def result = apiService.restoreServer(opts, vmId, snapshotId)

        then:
        1 * morpheusContext.executeWindowsCommand(
                'scvmm-server',
                5985,
                'admin',
                'password',
                { String cmd ->
                    cmd.contains('$VM = Get-SCVirtualMachine -VMMServer localhost -ID "vm-12345"') &&
                            cmd.contains('$Checkpoint = Get-SCVMCheckpoint -VM $VM | where {$_.Name -like "snapshot-6789"}') &&
                            cmd.contains('Restore-SCVMCheckpoint -VMCheckpoint $Checkpoint')
                },
                null,
                false
        ) >> Single.just([success: true, exitCode: '0', data: '{"Status":"Success"}'])

        result.success == true
    }

    @Unroll
    def "test buildCreateServerCommands generates correct PowerShell script"() {
        given:
        // Setup complete options for VM creation
        def opts = [
                // Basic VM configuration
                name: 'test-vm',
                hostname: 'test-hostname',
                memory: 4294967296L, // 4GB
                maxCpu: 2,
                maxCores: 4,

                // Dynamic memory settings
                minDynamicMemory: 2147483648L, // 2GB
                maxDynamicMemory: 8589934592L, // 8GB

                // VM identifiers
                vmId: 'vm-12345',
                cloneVMId: 'clone-vm-6789',
                imageId: 'img-9876',

                // VM type and generation
                scvmmCapabilityProfile: 'Hyper-V',
                scvmmGeneration: 'generation2',

                // Template information
                isTemplate: false,
                templateId: 'template-5678',

                // Storage configuration
                volumePath: 'C:\\ClusterStorage\\Volume1\\VMs',
                volumePaths: [
                        'C:\\ClusterStorage\\Volume1\\VMs\\test-vm\\disk0.vhdx',
                        'C:\\ClusterStorage\\Volume1\\VMs\\test-vm\\disk1.vhdx',
                        'C:\\ClusterStorage\\Volume1\\VMs\\test-vm\\disk2.vhdx'
                ],

                // Data disks configuration
                dataDisks: [
                        [name: 'data1', maxStorage: 10737418240L],
                        [name: 'data2', maxStorage: 21474836480L]
                ],

                // Disk external ID mappings for clone operations
                diskExternalIdMappings: [
                        'disk-ext-id-1',
                        'disk-ext-id-2'
                ],

                // Host and availability settings
                hostExternalId: 'host-ext-id-456',
                highlyAvailable: true,

                // Image configuration
                isSyncdImage: true,

                // Sysprep settings
                isSysprep: true,
                unattendPath: 'C:\\Temp\\unattend.xml',
                OSName: 'Windows Server 2019',

                // Zone/Region configuration
                zone: [
                        id: 10L,
                        name: 'Test Zone',
                        regionCode: 'us-east'
                ],

                // Network configuration
                networkConfig: [
                        doStatic: true,
                        primaryInterface: [
                                ipAddress: '192.168.1.100',
                                poolType: 'scvmm',
                                networkPool: [
                                        externalId: 'pool-123'
                                ],
                                vlanId: 42,
                                network: [
                                        externalId: 'network-abcd1234-5678-90ef-ghij-klmnopqrstuv'
                                ],
                                subnet: [
                                        externalId: 'subnet-abcd1234-5678-90ef-ghij-klmnopqrstuv'
                                ]
                        ]
                ]
        ]

        when:
        def result = apiService.buildCreateServerCommands(opts)

        then:
        // Verify the PowerShell script contains all expected commands
        result instanceof Map

        result.launchCommand != null
        result.launchCommand.contains('$VMNetwork = Get-SCVMNetwork -VMMServer localhost -ID "network-abcd1234-5678-90ef-ghij-klmn"')
        result.launchCommand.contains('$VMSubnet = Get-SCVMSubnet -VMMServer localhost -ID "subnet-abcd1234-5678-90ef-ghij-klmno"')
        result.launchCommand.contains('$MACAddressType = "Static"')
        result.launchCommand.contains('-VLanEnabled $true -VLanID 42')
        result.launchCommand.contains('-IPv4AddressType Static -IPv4Address "192.168.1.100"')
        result.launchCommand.contains('$vmHost = Get-SCVMHost -ID "host-ext-id-456"')
        result.launchCommand.contains('-HighlyAvailable $true')
        result.launchCommand.contains('-DynamicMemoryMinimumMB 2048')
        result.launchCommand.contains('-DynamicMemoryMaximumMB 8192')
        result.launchCommand.contains('$cloud = Get-SCCloud -ID "us-east"')
        result.launchCommand.contains('$VM = Get-SCVirtualMachine -VMMServer localhost -ID "clone-vm-6789"')
        result.launchCommand.contains('-Name "vm-12345"')

    }

    def "test buildCreateServerCommands generates correct hardware profile commands from template"() {
        given:
        // Setup complete options with template-specific settings
        def opts = [
                // Basic VM configuration
                name: 'test-vm',
                hostname: 'test-hostname',
                memory: 4294967296L, // 4GB
                maxCpu: 2,
                maxCores: 4,
                memoryMB: 4096,

                // Dynamic memory settings
                minDynamicMemory: 2147483648L, // 2GB
                maxDynamicMemory: 8589934592L, // 8GB
                minDynamicMemoryMB: 2048,
                maxDynamicMemoryMB: 8192,

                // Template-specific settings - these are needed to trigger our condition
                isTemplate: true,
                templateId: 'template-5678',
                hardwareProfileName: 'test-hw-profile',
                hardwareGuid: '{12345678-1234-5678-1234-567812345678}',
                generationNumber: 2,
                highlyAvailable: true,
                scvmmCapabilityProfile: 'Hyper-V',

                // Zone/Region configuration
                zone: [
                        id: 10L,
                        name: 'Test Zone',
                        regionCode: 'us-east'
                ],

                // Template values that should be preserved
                template: [
                        CPUExpectedUtilizationPercent: 30,
                        DiskIops: 500,
                        CPUMaximumPercent: 90,
                        NetworkUtilizationMbps: 100,
                        DynamicMemoryEnabled: true,
                        Memory: 2048,
                        DynamicMemoryMinimumMB: 1024,
                        DynamicMemoryMaximumMB: 4096,
                        DynamicMemoryBufferPercentage: 20,
                        FirstBootDevice: 'CD',
                        NumaIsolationRequired: true,
                        CPUPerVirtualNumaNodeMaximum: 2,
                        MemoryPerVirtualNumaNodeMaximumMB: 2048,
                        VirtualNumaNodesPerSocketMaximum: 2
                ],
                // Network configuration
                networkConfig: [
                        doStatic: true,
                        primaryInterface: [
                                ipAddress: '192.168.1.100',
                                poolType: 'scvmm',
                                networkPool: [
                                        externalId: 'pool-123'
                                ],
                                vlanId: 42,
                                network: [
                                        externalId: 'network-abcd1234-5678-90ef-ghij-klmnopqrstuv'
                                ],
                                subnet: [
                                        externalId: 'subnet-abcd1234-5678-90ef-ghij-klmnopqrstuv'
                                ]
                        ]
                ]
        ]

        when:
        def result = apiService.buildCreateServerCommands(opts)

        then:
        // Verify the PowerShell script contains expected hardware profile commands
        result instanceof Map
        result.launchCommand != null

        // Check for template and network components
        result.launchCommand.contains('\u0024template = Get-SCVMTemplate -VMMServer localhost | where {\u0024_.ID -eq "template-5678"}')
        result.launchCommand.contains('\u0024VMNetwork = Get-SCVMNetwork -VMMServer localhost -ID "network-abcd1234-5678-90ef-ghij-klmn"')
        result.launchCommand.contains('\u0024VMSubnet = Get-SCVMSubnet -VMMServer localhost -ID "subnet-abcd1234-5678-90ef-ghij-klmno"')
        // Check for network adapter config
        result.launchCommand.contains('\u0024MACAddressType = "Static"')
        result.launchCommand.contains('-VLanEnabled \u0024true -VLanID 42')
        // Check for IP configuration
        result.launchCommand.contains('-IPv4AddressType Static')
        result.launchCommand.contains('\u0024ipaddress = Get-SCIPAddress -IPAddress "192.168.1.100"')
        // Check for hardware profile settings
        result.launchCommand.contains('-HighlyAvailable \u0024true')
        result.launchCommand.contains('-DynamicMemoryMinimumMB 2048')
        result.launchCommand.contains('-DynamicMemoryMaximumMB 8192')
        // Check for cloud configuration
        result.launchCommand.contains('\u0024cloud = Get-SCCloud -ID "us-east"')
        // Check for VM creation command
        result.launchCommand.contains('\u0024createdVm = New-SCVirtualMachine')
        result.launchCommand.contains('\u0024createdVm | Select ID, ObjectType')
    }

    @Unroll
    def "test changeVolumeTypeForClonedBootDisk successfully changes volume type"() {
        given:
        def originalVMId = "vm-original-123"
        def newVMId = "vm-new-456"
        def opts = [
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]

        // Mock the command execution with a successful result
        morpheusContext.executeWindowsCommand(*_) >> Single.just([
                success: true,
                exitCode: '0',
                data: '{"Status":"Success"}'
        ])

        when:
        def result = apiService.changeVolumeTypeForClonedBootDisk(opts, originalVMId, newVMId)

        then:
        1 * morpheusContext.executeWindowsCommand(
                'scvmm-server',
                5985,
                'admin',
                'password',
                { String cmd ->
                    cmd.contains('$ClonedVM = Get-SCVirtualMachine -VMMServer localhost -ID "vm-original-123"') &&
                            cmd.contains('$OriginalBootDisk = Get-SCVirtualDiskDrive -VMMServer localhost -VM $ClonedVM | where {$_.VolumeType -eq "BootAndSystem"}') &&
                            cmd.contains('$NewVM = Get-SCVirtualMachine -VMMServer localhost -ID "vm-new-456"') &&
                            cmd.contains('$ClonedBootDisk = Get-SCVirtualDiskDrive -VMMServer localhost -VM $NewVM | where {$_.VirtualHardDisk -like [io.path]::GetFileNameWithoutExtension($OriginalBootDisk.VirtualHardDisk)}') &&
                            cmd.contains('Set-SCVirtualDiskDrive -VirtualDiskDrive $ClonedBootDisk -VolumeType BootAndSystem')
                },
                null,
                false
        ) >> Single.just([success: true, exitCode: '0', data: '{"Status":"Success"}'])

        result.success == true
    }

    @Unroll
    def "findBootDiskIndex returns correct index for boot disk when #scenario"() {
        given:
        def diskDrives = [
                disks: disksData
        ]

        when:
        def result = apiService.findBootDiskIndex(diskDrives)

        then:
        result == expectedIndex

        where:
        scenario                        | disksData                                                                               | expectedIndex
        "boot disk is first"            | [[VolumeType: 'BootAndSystem'], [VolumeType: 'Data']]                                   | 0
        "boot disk is second"           | [[VolumeType: 'Data'], [VolumeType: 'BootAndSystem'], [VolumeType: 'Data']]             | 1
        "boot disk is last"             | [[VolumeType: 'Data'], [VolumeType: 'Data'], [VolumeType: 'BootAndSystem']]             | 2
        "no boot disk exists"           | [[VolumeType: 'Data'], [VolumeType: 'Data']]                                            | 0
        "empty disks array"             | []                                                                                      | 0
        "disks with null VolumeType"    | [[VolumeType: null], [VolumeType: 'Data'], [VolumeType: 'BootAndSystem']]               | 2
        "case sensitive check"          | [[VolumeType: 'bootandsystem'], [VolumeType: 'Data'], [VolumeType: 'BootAndSystem']]    | 2
        "null disks property"           | null                                                                                    | 0
    }

    @Unroll
    def "test appendOSCustomization appends correct customization options for #scenario"() {
        given:
        def sourceString = "New-SCVirtualMachine -Name 'testvm'"
        def opts = [
                isSysprep: isSysprep,
                cloneVMId: cloneVMId,
                hostname: hostname,
                license: license
        ]

        when:
        def result = apiService.appendOSCustomization(sourceString, opts)

        then:
        result == expectedResult

        where:
        scenario                      | isSysprep | cloneVMId | hostname      | license                                                         | expectedResult
        "no sysprep"                  | false     | null      | "testhost"    | [fullName: "Test User", productKey: "ABCDE-12345", orgName: "TestOrg"] | "New-SCVirtualMachine -Name 'testvm'"
        "clone VM"                    | true      | "vm-123"  | "testhost"    | [fullName: "Test User", productKey: "ABCDE-12345", orgName: "TestOrg"] | "New-SCVirtualMachine -Name 'testvm'"
        "hostname only"               | true      | null      | "testhost"    | null                                                            | "New-SCVirtualMachine -Name 'testvm' -ComputerName \"testhost\""
        "license only"                | true      | null      | null          | [fullName: "Test User", productKey: "ABCDE-12345", orgName: "TestOrg"] | "New-SCVirtualMachine -Name 'testvm' -FullName \"Test User\" -ProductKey \"ABCDE-12345\" -OrganizationName \"TestOrg\""
        "partial license"             | true      | null      | null          | [fullName: "Test User", orgName: "TestOrg"]                     | "New-SCVirtualMachine -Name 'testvm' -FullName \"Test User\" -OrganizationName \"TestOrg\""
        "hostname and license"        | true      | null      | "testhost"    | [fullName: "Test User", productKey: "ABCDE-12345", orgName: "TestOrg"] | "New-SCVirtualMachine -Name 'testvm' -ComputerName \"testhost\" -FullName \"Test User\" -ProductKey \"ABCDE-12345\" -OrganizationName \"TestOrg\""
        "empty hostname"              | true      | null      | ""            | null                                                            | "New-SCVirtualMachine -Name 'testvm'"
        "all options"                 | true      | null      | "testhost"    | [fullName: "Test User", productKey: "ABCDE-12345", orgName: "TestOrg"] | "New-SCVirtualMachine -Name 'testvm' -ComputerName \"testhost\" -FullName \"Test User\" -ProductKey \"ABCDE-12345\" -OrganizationName \"TestOrg\""
    }

    @Unroll
    def "test generateDataDiskCommand with #scenario"() {
        given:
        def previousFileName = null
        def jobGuid = "job-123"

        when:
        def result = apiService.generateDataDiskCommand(
                busNumber, dataDiskNumber, jobGuid, sizeMB, path, fromDisk, discoverAvailableLUN, deployingToCloud
        )

        // If we need to test uniqueness, generate a second result with same params
        def secondResult = null
        if (testUniqueness) {
            secondResult = apiService.generateDataDiskCommand(
                    busNumber, dataDiskNumber, jobGuid, sizeMB, path, fromDisk, discoverAvailableLUN, deployingToCloud
            )
        }

        then:
        // Verify fileName format
        result.fileName.startsWith("data${dataDiskNumber}-")
        result.fileName.endsWith(".vhd")

        // Verify command structure based on parameters
        result.command.contains("-VMMServer localhost")
        result.command.contains("-${diskType}")
        result.command.contains("-Bus ${expectedBus}")
        result.command.contains("-LUN ${dataDiskNumber}")
        result.command.contains("-JobGroup ${jobGuid}")

        // Check path inclusion
        if (path && !deployingToCloud) {
            result.command.contains("-Path \"${path}\"")
        }

        // Check for size or fromDisk parameters
        if (fromDisk) {
            result.command.contains("-VirtualHardDisk ${fromDisk}")
            !result.command.contains("VirtualHardDiskSizeMB")
        } else {
            result.command.contains("-VirtualHardDiskSizeMB ${sizeMB}")
            result.command.contains("-Dynamic")
        }

        // Verify VolumeType inclusion
        result.command.contains("-VolumeType None")

        // Test uniqueness if required
        if (testUniqueness) {
            secondResult.fileName != result.fileName
            secondResult.fileName.startsWith("data${dataDiskNumber}-")
            secondResult.fileName.endsWith(".vhd")
        }

        where:
        scenario                               | busNumber | dataDiskNumber | sizeMB | path           | fromDisk       | discoverAvailableLUN | deployingToCloud | diskType | expectedBus | testUniqueness
        "default values"                       | "0"       | 1              | 10240  | null           | null           | false                | false            | "SCSI"   | "0"         | false
        "with path"                            | "0"       | 2              | 10240  | "C:\\VMs"      | null           | false                | false            | "SCSI"   | "0"         | false
        "custom bus"                           | "1"       | 3              | 10240  | null           | null           | false                | false            | "SCSI"   | "1"         | false
        "from existing disk"                   | "0"       | 4              | 10240  | null           | "existingDisk" | false                | false            | "SCSI"   | "0"         | false
        "from disk with path"                  | "0"       | 5              | 10240  | "C:\\VMs"      | "existingDisk" | false                | false            | "SCSI"   | "0"         | false
        "deploying to cloud"                   | "0"       | 6              | 10240  | null           | null           | false                | true             | "SCSI"   | "0"         | false
        "deploying to cloud with from disk"    | "0"       | 7              | 10240  | null           | "existingDisk" | true                 | true             | "SCSI"   | "0"         | false
        "test volume type and uniqueness"      | "0"       | 8              | 10240  | null           | null           | false                | false            | "SCSI"   | "0"         | true
    }

    @Unroll
    def "test getScvmmZoneOpts returns correct configuration for cloud"() {
        given:
        // Create cloud with account and config
        def account = new Account(id: 100L, name: "test-account")
        def cloud = new Cloud(
                id: 200L,
                name: "test-cloud",
                account: account,
                regionCode: "test-region"
        )

        // Set up cloud config
        def configMap = [libraryShare: "\\\\server\\share"]
        cloud.configMap = configMap

        // Mock the cloud.getConfigMap() method
        cloud.getConfigMap() >> configMap

        // Mock keyPair service and data query response
        def keyPair = new KeyPair(
                id: 300L,
                publicKey: "ssh-rsa AAAAB3NzaC1yc2E...",
                privateKey: "-----BEGIN RSA PRIVATE KEY-----\nMIIE..."
        )

        MorpheusSynchronousKeyPairService keyPairService = Mock(MorpheusSynchronousKeyPairService)
        def morphServ = morpheusContext.getServices()
        morphServ.getKeyPair() >> {
            return keyPairService
        }

        // Mock the find method to return our test keyPair
        keyPairService.find({ DataQuery query ->
            query.filters.any { it.name == "accountId" && it.value == 100L }
        }) >> keyPair

        when:
        def result = apiService.getScvmmZoneOpts(morpheusContext, cloud)

        then:
        result.account == account
        result.zoneConfig == configMap
        result.zone == cloud
        result.zoneId == 200L
        result.publicKey == "ssh-rsa AAAAB3NzaC1yc2E..."
        result.privateKey == "-----BEGIN RSA PRIVATE KEY-----\nMIIE..."
        result.rootSharePath == "\\\\server\\share"
        result.regionCode == "test-region"

        // Verify the keyPair service was called exactly once
        1 * keyPairService.find(_) >> keyPair
    }

    def "test getScvmmCloudOpts returns correctly populated cloud options"() {
        given:
        // Create cloud with account and config
        def account = new Account(id: 100L, name: "test-account")
        def cloud = new Cloud(
                id: 200L,
                name: "test-cloud",
                account: account,
                regionCode: "test-region"
        )

        // Create controller server
        def controllerServer = new ComputeServer(id: 300L, name: "test-controller")

        // Set up cloud config
        def configMap = [libraryShare: "\\\\server\\share", otherConfig: "value"]
        cloud.configMap = configMap

        // Mock the cloud.getConfigMap() method
        cloud.getConfigMap() >> configMap

        // Create test keyPair
        def keyPair = new KeyPair(
                id: 400L,
                publicKey: "ssh-rsa AAAAB3NzaC1yc2E...",
                privateKey: "-----BEGIN RSA PRIVATE KEY-----\nMIIE..."
        )

        // Mock keyPair service
        MorpheusSynchronousKeyPairService keyPairService = Mock(MorpheusSynchronousKeyPairService)
        morpheusContext.getServices().getKeyPair() >> keyPairService

        // Mock the keyPairService.find method to return our test keyPair
        keyPairService.find({ DataQuery query ->
            query.filters.any { it.name == "accountId" && it.value == 100L }
        }) >> keyPair

        when:
        def result = apiService.getScvmmCloudOpts(morpheusContext, cloud, controllerServer)

        then:
        // Verify all expected properties are in the result
        result.account == account
        result.zoneConfig == configMap
        result.zone == cloud
        result.zoneId == 200L
        result.publicKey == "ssh-rsa AAAAB3NzaC1yc2E..."
        result.privateKey == "-----BEGIN RSA PRIVATE KEY-----\nMIIE..."
        result.controllerServer == controllerServer
        result.rootSharePath == "\\\\server\\share"
        result.regionCode == "test-region"

        // Verify the keyPair service was called exactly once
        1 * keyPairService.find(_) >> keyPair
    }

    def "getScvmmControllerOpts correctly extracts controller options from server"() {
        given:
        def cloud = new Cloud(id: 1L)
        def server = new ComputeServer(
                id: 10L,
                name: "controller-01",
                internalIp: "10.0.0.10",
                sshUsername: "admin",
                sshPassword: "securepass123",
                sshHost: "10.0.0.10"
        )

        // Set the hypervisorConfig with the actual paths that match the implementation
        server.setConfigProperty("hypervisorConfig", [
                workingPath: "D:\\Working",
                diskPath: "D:\\Disks"
        ])

        when:
        def result = apiService.getScvmmControllerOpts(cloud, server)

        then:
        result.sshHost == "10.0.0.10"
        result.sshUsername == "admin"
        result.sshPassword == "securepass123"
        result.hypervisor == server
    }

    @Unroll
    def "test getScvmmZoneAndHypervisorOpts correctly combines options from cloud and controller"() {
        given:
        // Create test objects
        def cloud = new Cloud(id: 200L, name: "test-cloud")
        def hypervisor = new ComputeServer(id: 300L, name: "test-hypervisor")

        // Mock the return values for the two component methods
        def cloudOpts = [
                zoneId: 200L,
                cloudName: "test-cloud",
                regionCode: "us-east-1",
                rootSharePath: "\\\\server\\share"
        ]

        def controllerOpts = [
                sshHost: "10.0.0.10",
                sshUsername: "admin",
                sshPassword: "password123",
                hypervisor: hypervisor
        ]

        // Mock the methods that are called inside getScvmmZoneAndHypervisorOpts
        apiService.getScvmmCloudOpts(_, _, _) >> {
            return cloudOpts
        }
        apiService.getScvmmControllerOpts(_, _) >> {
            return controllerOpts
        }

        when:
        def result = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, hypervisor)

        then:
        // Verify that result contains combined options from both methods
        result.zoneId == 200L
        result.cloudName == "test-cloud"
        result.regionCode == "us-east-1"
        result.rootSharePath == "\\\\server\\share"
        result.sshHost == "10.0.0.10"
        result.sshUsername == "admin"
        result.sshPassword == "password123"
        result.hypervisor == hypervisor

        // Verify each method was called exactly once with the correct parameters
        1 * apiService.getScvmmCloudOpts(_, _, _) >> cloudOpts
        1 * apiService.getScvmmControllerOpts(_, _) >> controllerOpts
    }

    @Unroll
    def "loadControllerServer correctly loads controller server when controllerServerId is #scenario"() {
        given:
        // Create a test options map
        def controllerId = 123L
        def opts = [controllerServerId: controllerId]

        // Create a server to be returned by the service
        def serverToReturn = controllerId ? new ComputeServer(id: controllerId, name: "controller-server") : null

        // Mock the computeServerService get method to return our test server

        computeServerService.get(controllerId) >> serverToReturn

        when:
        apiService.loadControllerServer(opts)

        then:
        // Verify the result based on whether a controllerId was provided

        opts.controllerServer == serverToReturn
//        opts.controllerServer.id == controllerId
//        opts.controllerServer.name == "controller-server"
        1 * computeServerService.get(controllerId) >> serverToReturn

    }

    @Unroll
    def "test isHostInHostGroup with currentPath=#currentPath and testPath=#testPath returns #expectedResult"() {
        when:
        def result = apiService.isHostInHostGroup(currentPath, testPath)

        then:
        result == expectedResult

        where:
        currentPath              | testPath               | expectedResult | scenario
        "HostGroup\\Host"        | "HostGroup"            | true          | "host is direct child of hostgroup"
        "HostGroup\\SubGroup\\Host" | "HostGroup"        | true          | "host is in nested subgroup"
        "HostGroup"              | "HostGroup"            | true          | "paths are identical"
        "HostGroup2\\Host"       | "HostGroup"            | false         | "different host groups"
        "HostGroupX"             | "HostGroup"            | false         | "similar prefix but not in group"
        "HostGroup\\Host"        | null                   | false         | "null test path"
        ""                       | "HostGroup"            | false         | "empty current path"
        "HostGroup"              | ""                     | false         | "empty test path"
        "HostGroup\\Host"        | "hostgroup"            | false         | "case sensitive comparison"
    }

    @Unroll
    def "test getScvmmInitializationOpts with #scenario"() {
        given:
        // Create a cloud with configuration
        def cloud = new Cloud(id: 1L, name: "test-cloud")
        def configMap = [
                host: hostValue,
                diskPath: diskPath,
                workingPath: workingPath
        ]
        cloud.setAccountCredentialLoaded(true)
        def actCredDataMap =  [username:"dunno", password: "testpass"]
        cloud.setAccountCredentialData(actCredDataMap)
        cloud.servicePassword = "testpass"
        cloud.serviceUsername = "dunno"

        // Mock getConfigMap to return our test config
        cloud.setConfig(configMap.toString())

        // Set the defaultRoot field to test fallbacks
        apiService.defaultRoot = "C:\\MorpheusData"

        when:
        def result = apiService.getScvmmInitializationOpts(cloud)

        then:
        result.sshUsername == "dunno"
        result.sshPassword == "testpass"

        where:
        scenario                   | hostValue     | diskPath             | workingPath           | expectedZoneRoot       | expectedDiskRoot
        "all paths specified"      | "10.0.0.10"   | "D:\\CustomDisks"    | "D:\\CustomWorking"   | "D:\\CustomWorking"    | "D:\\CustomDisks"
        "missing disk path"        | "10.0.0.11"   | ""                   | "D:\\CustomWorking"   | "D:\\CustomWorking"    | "C:\\MorpheusData\\Disks"
        "missing working path"     | "10.0.0.12"   | "D:\\CustomDisks"    | ""                    | "C:\\MorpheusData"     | "D:\\CustomDisks"
        "missing both paths"       | "10.0.0.13"   | ""                   | ""                    | "C:\\MorpheusData"     | "C:\\MorpheusData\\Disks"
        "null disk path"           | "10.0.0.14"   | null                 | "D:\\CustomWorking"   | "D:\\CustomWorking"    | "C:\\MorpheusData\\Disks"
        "null working path"        | "10.0.0.15"   | "D:\\CustomDisks"    | null                  | "C:\\MorpheusData"     | "D:\\CustomDisks"
        "null both paths"          | "10.0.0.16"   | null                 | null                  | "C:\\MorpheusData"     | "C:\\MorpheusData\\Disks"
    }

    @Unroll
    def "test getUsername returns #expectedUsername when #scenario"() {
        given:
        def cloud = new Cloud(id: 1L)
        cloud.accountCredentialLoaded = credentialLoaded
        if (hasCredentialData) {
            cloud.accountCredentialData = [username: credentialUsername]
        }
        if (hasConfigProperty) {
            cloud.setConfigProperty('username', configUsername)
        }

        when:
        def result = apiService.getUsername(cloud)

        then:
        result == expectedUsername

        where:
        scenario                                | credentialLoaded | hasCredentialData | credentialUsername | hasConfigProperty | configUsername | expectedUsername
        "credentials loaded with username"      | true             | true              | "credential-user"  | false             | null           | "credential-user"
        "credentials not loaded with config"    | false            | false             | null               | true              | "config-user"  | "config-user"
        "no credentials or config"              | false            | false             | null               | false             | null           | "dunno"
        "empty config falls to default"         | false            | false             | null               | true              | ""             | "dunno"
    }

    @Unroll
    def "test getPassword returns #expectedPassword when #scenario"() {
        given:
        def cloud = new Cloud(id: 1L)
        cloud.accountCredentialLoaded = credentialLoaded
        if (hasCredentialData) {
            cloud.accountCredentialData = [password: credentialPassword]
        }
        if (hasConfigProperty) {
            cloud.setConfigProperty('password', configPassword)
        }

        when:
        def result = apiService.getPassword(cloud)

        then:
        result == expectedPassword

        where:
        scenario                                | credentialLoaded | hasCredentialData | credentialPassword | hasConfigProperty | configPassword | expectedPassword
        "credentials loaded with password"      | true             | true              | "credential-pass"  | false             | null           | "credential-pass"
        "credentials not loaded with config"    | false            | false             | null               | true              | "config-pass"  | "config-pass"
        "no credentials or config"              | false            | false             | null               | false             | null           | null
        "empty config returns null"             | false            | false             | null               | true              | ""             | ""
        "credentials loaded but null data"      | true             | false             | null               | true              | "config-pass"  | "config-pass"
    }

    @Unroll
    def "test deleteImage successfully removes image directory"() {
        given:
        def imageName = "test-image"
        def opts = [
                zoneRoot: "C:\\Temp",
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]

        // Spy on formatImageFolder to return a known value
        apiService.formatImageFolder(imageName) >> "test-image-folder"

        // Mock the command execution with a successful result
        morpheusContext.executeWindowsCommand(*_) >> Single.just([
                success: true,
                exitCode: '0',
                data: '{"Status":"Success"}'
        ])

        when:
        def result = apiService.deleteImage(opts, imageName)

        then:
        // Verify formatImageFolder was called with the image name
        1 * apiService.formatImageFolder(imageName) >> "test-image-folder"

        // Verify generateCommandString was called with the correct command
        1 * apiService.generateCommandString("Remove-Item -LiteralPath \"C:\\Temp\\images\\test-image-folder\" -Recurse -Force") >> "powershell -command \"Remove-Item -LiteralPath \\\"C:\\Temp\\images\\test-image-folder\\\" -Recurse -Force\""

        // Verify executeWindowsCommand was called with the correct parameters
        1 * morpheusContext.executeWindowsCommand(
                'scvmm-server',
                5985,
                'admin',
                'password',
                "powershell -command \"Remove-Item -LiteralPath \\\"C:\\Temp\\images\\test-image-folder\\\" -Recurse -Force\"",
                null,
                false
        ) >> Single.just([success: true, exitCode: '0', data: '{"Status":"Success"}'])

        // Verify the result was successful
        result.success == true
    }

    @Unroll
    def "test deleteImage uses defaultRoot when zoneRoot is not provided"() {
        given:
        def imageName = "test-image"
        def opts = [
                // No zoneRoot specified
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]

        // Set the defaultRoot value
        apiService.defaultRoot = "C:\\MorpheusData"

        // Spy on formatImageFolder to return a known value
        apiService.formatImageFolder(imageName) >> "test-image-folder"

        // Mock the wrapExecuteCommand to return success
        apiService.wrapExecuteCommand(_, opts) >> [success: true, data: '{"Status":"Success"}']

        when:
        def result = apiService.deleteImage(opts, imageName)

        then:
        // Verify generateCommandString was called with a command that uses the defaultRoot
        1 * apiService.generateCommandString({ String cmd ->
            cmd.contains("C:\\MorpheusData\\images\\test-image-folder")
        }) >> "powershell command string"

        // Verify the result was successful
        result.success == true
    }

    @Unroll
    def "test findImage correctly detects if an image exists"() {
        given:
        def imageName = "test-image"
        def formattedImageFolder = "test_image"
        def zoneRoot = "C:\\SCVMM"
        def imageFolderPath = "${zoneRoot}\\images\\${formattedImageFolder}"
        def opts = [
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985',
                zoneRoot: zoneRoot
        ]
        def mockedResponse = imageExists ? "C:\\SCVMM\\images\\test_image\\disk.vhd" : ""

        when:
        def result = apiService.findImage(opts, imageName)

        then:
        1 * apiService.executeCommand(
                { String cmd ->
                    cmd == "(Get-ChildItem -File \"${imageFolderPath}\").FullName"
                },
                opts
        ) >> [success: true, data: mockedResponse]

        1 * apiService.formatImageFolder(imageName) >> formattedImageFolder

        result.success == true
        result.imageExists == imageExists
        if (imageExists) {
            result.imageName == "C:\\SCVMM\\images\\test_image\\disk.vhd"
        }

        where:
        imageExists << [true, false]
    }

    @Unroll
    def "test getMapScvmmOsType with #scenario"() {

        when:
        def result = apiService.getMapScvmmOsType(searchFor, findByKey, defaultOsType)

        then:
        result == expectedResult

        where:
        scenario                                  | searchFor                                     | findByKey | defaultOsType | expectedResult
        "finding by exact key match"              | "Windows Server 2016 Datacenter"              | true      | null          | "windows.server.2016"
        "finding by key with no match uses other" | "Non-existent OS"                            | true      | null          | "other"
        "finding by key with default fallback"    | "Non-existent OS"                            | true      | ""            | "other"
        "finding by value with exact match"       | "windows.server.2019"                         | false     | null          | "Windows Server 2019 Datacenter"
        "finding by value with no match"          | "non.existent.os"                             | false     | null          | null
        "finding Linux OS by key"                 | "Ubuntu Linux 20.04 (64 bit)"                | true      | null          | "ubuntu.20.04.64"
        "finding Windows OS by key"               | "64-bit edition of Windows Server 2012 Datacenter" | true | null          | "windows.server.2012"
        "finding with empty key"                  | ""                                            | true      | null          | "other"
        "finding with null key"                   | null                                          | true      | null          | "other"
        "finding by value with multiple matching keys" | "windows.server.2025"                    | false     | null          | "Windows Server 2025 Datacenter"
    }

    @Unroll
    def "test cleanData with #scenario"() {
        given:
        def data = inputData
        def ignore = ignoreString

        when:
        def result = apiService.cleanData(data, ignore)

        then:
        result == expectedResult

        where:
        scenario                      | inputData                                  | ignoreString | expectedResult
        "null data"                   | null                                       | null         | ""
        "empty data"                  | ""                                         | null         | ""
        "single line"                 | "test data"                                | null         | "test data"
        "single line with whitespace" | "   test data   "                          | null         | "test data"
        "multiple lines"              | "line1\nline2\nline3"                      | null         | "line3"
        "short lines filtered out"    | "a\nline2\n.\nline4"                       | null         | "line4"
    }

    @Unroll
    def "test updateServer handles different update scenarios correctly for #scenario"() {
        given:
        def vmId = "vm-12345"
        def opts = [
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]
        def updates = providedUpdates

        // Set up mocks for the command execution
        def expectedCommandPattern = commandPattern
        def commandOutput = [success: true, exitCode: '0', data: '{"Status":"Success"}']

        when:
        def result = apiService.updateServer(opts, vmId, updates)

        then:
        if (shouldCallCommand) {
            1 * apiService.wrapExecuteCommand(_, _) >> commandOutput
        } else {
            0 * apiService.generateCommandString(_)
            0 * apiService.wrapExecuteCommand(_, _)
        }

        result.success == true

        where:
        scenario                    | providedUpdates                                           | commandPattern                                           | shouldCallCommand
//        "no updates"                | [:]                                                       | []                                                        | false
        "update CPU only"           | [maxCores: 4]                                             | ["-CPUCount 4"]                                           | true
        "update memory only"        | [maxMemory: 8589934592L]                                  | ["\$maxMemory = 8192", "DynamicMemoryEnabled \$false"]    | true
        "update memory and CPU"     | [maxMemory: 4294967296L, maxCores: 2]                     | ["-CPUCount 2", "\$maxMemory = 4096"]                     | true
        "update with min memory"    | [maxMemory: 4294967296L, minDynamicMemory: 2147483648L]   | ["\$minDynamicMemory = 2048", "DynamicMemoryEnabled \$true"] | true
        "update with max memory"    | [maxMemory: 4294967296L, maxDynamicMemory: 8589934592L]   | ["\$maxDynamicMemory = 8192", "DynamicMemoryEnabled \$true"] | true
        "update with all memory"    | [maxMemory: 4294967296L, minDynamicMemory: 2147483648L, maxDynamicMemory: 8589934592L] | ["\$minDynamicMemory = 2048", "\$maxDynamicMemory = 8192", "DynamicMemoryEnabled \$true"] | true
//        "just min dynamic memory"   | [minDynamicMemory: 2147483648L]                           | ["\$minDynamicMemory = 2048"]                             | true
//        "just max dynamic memory"   | [maxDynamicMemory: 8589934592L]                           | ["\$maxDynamicMemory = 8192"]                             | true
    }

    @Unroll
    def "test validateServerConfig with #scenario"() {
        given:
        def opts = inputOpts

        when:
        def result = apiService.validateServerConfig(opts)

        then:
        result.success == expectedSuccess
        if (!expectedSuccess) {
            result.errors.size() == expectedErrorCount
            result.errors.any { it.field == expectedErrorField && it.msg == expectedErrorMsg }
        }

        where:
        scenario                                | inputOpts                                                                                  | expectedSuccess | expectedErrorCount | expectedErrorField      | expectedErrorMsg
        "missing capability profile"            | [:]                                                                                        | false           | 2                  | "scvmmCapabilityProfile"| "You must select a capability profile"
        "missing network"                       | [scvmmCapabilityProfile: "Hyper-V"]                                                        | false           | 1                  | "networkId"             | "Network is required"
        "empty nodeCount"                       | [scvmmCapabilityProfile: "Hyper-V", networkId: "net-123", nodeCount: ""]                   | false           | 1                  | "nodeCount"             | "You must indicate number of hosts"
        "valid config with networkId"           | [scvmmCapabilityProfile: "Hyper-V", networkId: "net-123"]                                  | true            | 0                  | null                    | null
        //"valid config with network interfaces"  | [scvmmCapabilityProfile: "Hyper-V", networkInterfaces: [[network: [id: "net-456"]]]]       | true            | 0                  | null                    | null
        "missing network id in interface"       | [scvmmCapabilityProfile: "Hyper-V", networkInterfaces: [[network: [:]]]]                   | false           | 1                  | "networkInterface"      | "Network is required"
        "missing ip address for static"         | [scvmmCapabilityProfile: "Hyper-V", networkInterfaces: [[network: [id: "net-789"], ipMode: "static"]]]   | false | 1 | "networkInterface" | "You must enter an ip address"
        //"direct networkInterface config"        | [scvmmCapabilityProfile: "Hyper-V", networkInterface: [network: [id: ["net-abc"]]]]        | true            | 0                  | null                    | null
        "invalid networkInterface config"       | [scvmmCapabilityProfile: "Hyper-V", networkInterface: [network: [id: [""]]]]               | false           | 1                  | "networkInterface"      | "Network is required"
        "static IP missing in networkInterface" | [scvmmCapabilityProfile: "Hyper-V", networkInterface: [network: [id: ["net-def"]], ipMode: ["static"], ipAddress: [null]]] | false | 1 | "networkInterface" | "You must enter an ip address"
    }

//    @Unroll
//    def "test importAndMountIso successfully imports and mounts ISO file"() {
//        given:
//        def diskFolder = "C:\\Temp\\VMs\\test-vm"
//        def imageFolderName = "test-image"
//
//        def cloudConfigBytes = "test-cloud-config-content".bytes
//        def opts = [
//                hypervisor: Mock(ComputeServer) {
//                    getName() >> "test-hypervisor"
//                },
//                sshHost: 'scvmm-server',
//                sshUsername: 'admin',
//                sshPassword: 'password'
//        ]
//
//        // Expected path for the ISO file
//        def isoPath = "${diskFolder}\\config.iso"
//        def expectedSharePath = "\\\\server\\share\\config.iso"
//
//        // Set up morpheusContext services mock
//        morpheusContext.getServices() >> Mock(MorpheusServices) {
//            getFileCopy() >> fileCopyService
//        }
//        // Mock fileCopy service
//        def copyToServerResponse = new ServiceResponse(success: true)
//
//
//        // Mock importPhysicalResource
//        def importResponse = [success: true, sharePath: expectedSharePath]
//        apiService.importPhysicalResource(_, _, _, _) >> importResponse
//
//        // Mock the directory creation command
//        apiService.generateCommandString("\$ignore = mkdir \"${diskFolder}\"") >> "powershell mkdir command"
//        apiService.wrapExecuteCommand("powershell mkdir command", opts) >> [success: true]
//
//        fileCopyService.copyToServer( opts.hypervisor,
//                "config.iso",
//                isoPath,
//                { InputStream is -> is instanceof ByteArrayInputStream },
//                cloudConfigBytes.size()) >> {
//            return copyToServerResponse
//        }
//
//        // Mock setCdrom
//        apiService.setCdrom(_, _) >> [success: true]
//
//
//        // Mock the actual implementation method to use encodeBase64 instead of encodeAsBase64
//        ScvmmApiService.metaClass.static.importAndMountIso = { bytes, folder, imageFolder, options ->
//            // Use the correct encoding method
//            bytes.encodeBase64()
//            return expectedSharePath
//        }
//        when:
//        def result = apiService.importAndMountIso(cloudConfigBytes, diskFolder, imageFolderName, opts)
//
//        then:
//
//        // Verify the return value
//        result == expectedSharePath
//
//        // Verify command generation for directory creation
//        1 * apiService.generateCommandString("\$ignore = mkdir \"${diskFolder}\"")
//
//        // Verify file copy was called with correct parameters
//        1 * fileCopyService.copyToServer(
//                opts.hypervisor,
//                "config.iso",
//                isoPath,
//                { it instanceof ByteArrayInputStream },
//                cloudConfigBytes.size()
//        )
//
//        // Verify importPhysicalResource was called with correct parameters
//        1 * apiService.importPhysicalResource(
//                opts,
//                isoPath,
//                imageFolderName,
//                'config.iso'
//        )
//
//        // Verify setCdrom was called with correct parameters
//        1 * apiService.setCdrom(opts, expectedSharePath)
//
//    }
//
//    @Unroll
//    def "test importAndMountIso throws exception when file copy fails"() {
//        given:
//        def diskFolder = "C:\\Temp\\VMs\\test-vm"
//        def imageFolderName = "test-image"
//        def cloudConfigBytes = "test-cloud-config-content".bytes
//        def hypervisorName = "test-hypervisor"
//        def opts = [
//                hypervisor: Mock(ComputeServer) {
//                    getName() >> hypervisorName
//                },
//                sshHost: 'scvmm-server',
//                sshUsername: 'admin',
//                sshPassword: 'password'
//        ]
//
//        // Mock fileCopy service to return failure
//        def copyToServerResponse = new ServiceResponse(success: false)
//        fileCopyService.copyToServer(_, _, _, _, _) >> copyToServerResponse
//
//        // Mock the directory creation command
//        apiService.generateCommandString(_) >> "powershell command"
//        apiService.wrapExecuteCommand(_, _) >> [success: true]
//
//        when:
//        apiService.importAndMountIso(cloudConfigBytes, diskFolder, imageFolderName, opts)
//
//        then:
//        // Verify exception was thrown with correct message
//        def exception = thrown(Exception)
//        exception.message == "ISO Upload to SCVMM Host Failed. Perhaps an agent communication issue...${hypervisorName}"
//
//        // Verify command generation for directory creation
//        1 * apiService.generateCommandString("\$ignore = mkdir \"${diskFolder}\"")
//
//        // Verify file copy was called but other methods weren't
//        1 * fileCopyService.copyToServer(_, _, _, _, _) >> copyToServerResponse
//        0 * apiService.importPhysicalResource(_, _, _, _)
//        0 * apiService.setCdrom(_, _)
//    }

    def "test createDVD successfully creates DVD drive"() {
        given:
        def opts = [
                externalId: "vm-12345",
                scvmmGeneration: generation,
                sshHost: 'scvmm-server',
                sshUsername: 'admin',
                sshPassword: 'password',
                winrmPort: '5985'
        ]

        // Mock the command execution with a successful result
        def expectedResponse = [success: true, data: '{"success":true,"BUS":' + expectedBus + ',"LUN":' + expectedLun + '}']
        apiService.wrapExecuteCommand(_, opts) >> expectedResponse

        when:
        apiService.createDVD(opts)

        then:
        // Verify generateCommandString was called with the correct command
        1 * apiService.generateCommandString({ String cmd ->
            cmd.contains('$busNumber = ' + expectedBus) &&
                    cmd.contains('$lunNumber = ' + expectedLun) &&
                    cmd.contains('$externalId = "vm-12345"') &&
                    cmd.contains('New-SCVirtualDVDDrive -VMMServer localhost -JobGroup $jobGuid -Bus $busNumber -LUN $lunNumber')
        }) >> "powershell command"

        // Verify wrapExecuteCommand was called with the generated command
        1 * apiService.wrapExecuteCommand("powershell command", opts) >> expectedResponse

        where:
        generation     | expectedBus | expectedLun
        'generation1'  | 0           | 0
        'generation2'  | 0           | 1
    }

    @Unroll
    def "test importScript successfully imports script file"() {
        given:
        // Mock a ComputeServer
        def mockedComputerServer = Mock(ComputeServer) {
            getName() >> "test-server"
        }

        // Setup test data
        def content = "#!/bin/bash\necho Hello World"
        def diskFolder = "C:\\Temp\\Scripts"
        def imageFolderName = "test-script-folder"
        def fileName = "setup.sh"
        def sharePath = "\\\\server\\share\\setup.sh"

        def opts = [
                zoneRoot: "C:\\Temp",
                sshHost: 'localhost',
                sshUsername: 'admin',
                sshPassword: 'password',
                hypervisor: mockedComputerServer,
                fileName: fileName,
                cloudConfigBytes: content.getBytes()
        ]

        // Mock the fileCopy service response
        def serviceResp = new ServiceResponse(success: true)

        // Mock the directory creation command
        apiService.generateCommandString("\$ignore = mkdir \"${diskFolder}\"") >> "mkdir command"
        apiService.wrapExecuteCommand("mkdir command", opts) >> [success: true]

        // Mock the fileCopy service
        morpheusContext.getServices() >> Mock(MorpheusServices) {
            getFileCopy() >> fileCopyService
        }

        // Mock the importPhysicalResource method
        apiService.importPhysicalResource(opts, "${diskFolder}\\${fileName}", imageFolderName, fileName) >> [
                success: true,
                sharePath: sharePath
        ]

        when:
        def result = apiService.importScript(content, diskFolder, imageFolderName, opts)

        then:
        // Verify directory creation command was generated correctly
        1 * apiService.generateCommandString("\$ignore = mkdir \"${diskFolder}\"") >> "mkdir command"

        // Verify directory creation was executed
        1 * apiService.wrapExecuteCommand("mkdir command", opts) >> [success: true]

        // Verify file copy was called with correct parameters
        1 * fileCopyService.copyToServer(
                opts.hypervisor,
                fileName,
                "${diskFolder}\\${fileName}",
                { InputStream is ->
                    // Verify that input stream contains the expected content
                    String streamContent = new String(is.bytes)
                    streamContent == content
                },
                content.bytes.size(),
                null,
                true
        ) >> serviceResp

        // Verify importPhysicalResource was called with correct parameters
        1 * apiService.importPhysicalResource(
                opts,
                "${diskFolder}\\${fileName}",
                imageFolderName,
                fileName
        ) >> [success: true, sharePath: sharePath]

        // Verify the result is the expected share path
        result == sharePath
    }
}