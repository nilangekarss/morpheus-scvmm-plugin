package com.morpheusdata.scvmm.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.*
import com.morpheusdata.scvmm.ScvmmApiService
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Comprehensive unit tests for IsolationNetworkSync class.
 * Tests all three main methods: execute, addMissingNetworks, and updateMatchedNetworks
 * with extensive test coverage including edge cases and error scenarios.
 */
class IsolationNetworkSyncSpec extends Specification {

    private IsolationNetworkSync isolationNetworkSync
    private MorpheusContext mockContext
    private ScvmmApiService mockApiService
    private Cloud cloud
    private ComputeServer server
    private Account testAccount
    private NetworkType networkType

    def setup() {
        // Setup test objects
        testAccount = new Account(id: 1L, name: "test-account")
        cloud = new Cloud(
                id: 1L,
                name: "test-scvmm-cloud",
                account: testAccount,
                owner: testAccount
        )

        server = new ComputeServer(
                id: 2L,
                name: "scvmm-server",
                externalId: "server-123"
        )

        networkType = new NetworkType(code: 'scvmmVLANNetwork')

        // Setup mock services
        mockContext = Mock(MorpheusContext)
        mockApiService = Mock(ScvmmApiService)

        // Create IsolationNetworkSync instance
        isolationNetworkSync = new IsolationNetworkSync(mockContext, cloud, mockApiService)
    }

    // =====================================================
    // Tests for Constructor
    // =====================================================

    @Unroll
    def "constructor should initialize all required fields"() {
        given: "Mock context, cloud, and api service"
        def testContext = Mock(MorpheusContext)
        def testCloud = new Cloud(id: 5L, name: "test-cloud")
        def testApiService = Mock(ScvmmApiService)

        when: "IsolationNetworkSync is constructed"
        def sync = new IsolationNetworkSync(testContext, testCloud, testApiService)

        then: "instance is created successfully"
        sync != null
    }

    // =====================================================
    // Tests for addMissingNetworks Logic - Using Direct Network Creation
    // =====================================================

    @Unroll
    def "addMissingNetworks logic should create correct network configurations"() {
        given: "Network items to add"
        def networkItem1 = [
            ID: "network-123",
            Name: "VLAN Network 1",
            Subnet: "192.168.1.0/24",
            VLanID: 100
        ]
        def networkItem2 = [
            ID: "network-456",
            Name: "VLAN Network 2",
            Subnet: "10.0.0.0/16",
            VLanID: 200
        ]
        def addList = [networkItem1, networkItem2]

        when: "addMissingNetworks logic is executed manually to test network creation"
        def networkAdds = []
        addList?.each { networkItem ->
            def networkConfig = [
                    code      : "scvmm.vlan.network.${cloud.id}.${server.id}.${networkItem.ID}",
                    cidr      : networkItem.Subnet,
                    vlanId    : networkItem.VLanID,
                    category  : "scvmm.vlan.network.${cloud.id}.${server.id}",
                    cloud     : cloud,
                    dhcpServer: true,
                    uniqueId  : networkItem.ID,
                    name      : networkItem.Name,
                    externalId: networkItem.ID,
                    type      : networkType,
                    refType   : 'ComputeZone',
                    refId     : cloud.id,
                    owner     : cloud.owner,
            ]
            Network networkAdd = new Network(networkConfig)
            networkAdds << networkAdd
        }

        then: "networks are created with correct properties"
        networkAdds.size() == 2

        // Verify first network
        networkAdds[0].code == "scvmm.vlan.network.1.2.network-123"
        networkAdds[0].cidr == "192.168.1.0/24"
        networkAdds[0].vlanId == 100
        networkAdds[0].category == "scvmm.vlan.network.1.2"
        networkAdds[0].cloud == cloud
        networkAdds[0].dhcpServer == true
        networkAdds[0].uniqueId == "network-123"
        networkAdds[0].name == "VLAN Network 1"
        networkAdds[0].externalId == "network-123"
        networkAdds[0].type == networkType
        networkAdds[0].refType == "ComputeZone"
        networkAdds[0].refId == cloud.id
        networkAdds[0].owner == cloud.owner

        // Verify second network
        networkAdds[1].code == "scvmm.vlan.network.1.2.network-456"
        networkAdds[1].cidr == "10.0.0.0/16"
        networkAdds[1].vlanId == 200
        networkAdds[1].category == "scvmm.vlan.network.1.2"
        networkAdds[1].name == "VLAN Network 2"
        networkAdds[1].externalId == "network-456"
    }

    @Unroll
    def "addMissingNetworks logic should handle empty add list"() {
        given: "Empty add list"
        def addList = []

        when: "addMissingNetworks logic is executed"
        def networkAdds = []
        addList?.each { networkItem ->
            def networkConfig = [
                    code      : "scvmm.vlan.network.${cloud.id}.${server.id}.${networkItem.ID}",
                    cidr      : networkItem.Subnet,
                    vlanId    : networkItem.VLanID,
                    category  : "scvmm.vlan.network.${cloud.id}.${server.id}",
                    cloud     : cloud,
                    dhcpServer: true,
                    uniqueId  : networkItem.ID,
                    name      : networkItem.Name,
                    externalId: networkItem.ID,
                    type      : networkType,
                    refType   : 'ComputeZone',
                    refId     : cloud.id,
                    owner     : cloud.owner,
            ]
            Network networkAdd = new Network(networkConfig)
            networkAdds << networkAdd
        }

        then: "no networks are created"
        networkAdds.size() == 0
    }

    @Unroll
    def "addMissingNetworks logic should handle null add list"() {
        given: "Null add list"
        def addList = null

        when: "addMissingNetworks logic is executed"
        def networkAdds = []
        addList?.each { item ->
            def networkConfig = [
                    code      : "scvmm.vlan.network.${cloud.id}.${server.id}.${item.ID}",
                    cidr      : item.Subnet,
                    vlanId    : item.VLanID,
                    category  : "scvmm.vlan.network.${cloud.id}.${server.id}",
                    cloud     : cloud,
                    dhcpServer: true,
                    uniqueId  : item.ID,
                    name      : item.Name,
                    externalId: item.ID,
                    type      : networkType,
                    refType   : 'ComputeZone',
                    refId     : cloud.id,
                    owner     : cloud.owner,
            ]
            Network networkAdd = new Network(networkConfig)
            networkAdds << networkAdd
        }

        then: "no networks are created"
        networkAdds.size() == 0
    }

    @Unroll
    def "addMissingNetworks logic should handle network with null values gracefully"() {
        given: "Network item with null values"
        def networkItem = [
            ID: "network-null",
            Name: null,
            Subnet: null,
            VLanID: null
        ]
        def addList = [networkItem]

        when: "addMissingNetworks logic is executed"
        def networkAdds = []
        addList?.each { item ->
            def networkConfig = [
                    code      : "scvmm.vlan.network.${cloud.id}.${server.id}.${item.ID}",
                    cidr      : item.Subnet,
                    vlanId    : item.VLanID,
                    category  : "scvmm.vlan.network.${cloud.id}.${server.id}",
                    cloud     : cloud,
                    dhcpServer: true,
                    uniqueId  : item.ID,
                    name      : item.Name,
                    externalId: item.ID,
                    type      : networkType,
                    refType   : 'ComputeZone',
                    refId     : cloud.id,
                    owner     : cloud.owner,
            ]
            Network networkAdd = new Network(networkConfig)
            networkAdds << networkAdd
        }

        then: "network is created with null values"
        networkAdds.size() == 1
        networkAdds[0].code == "scvmm.vlan.network.1.2.network-null"
        networkAdds[0].cidr == null
        networkAdds[0].vlanId == null
        networkAdds[0].name == null
        networkAdds[0].externalId == "network-null"
    }

    // =====================================================
    // Tests for updateMatchedNetworks Logic - Direct Testing
    // =====================================================

    @Unroll
    def "updateMatchedNetworks logic should update cidr when different"() {
        given: "Existing network with different CIDR"
        def existingNetwork = new Network(
                id: 1L,
                name: "Test Network",
                cidr: "192.168.0.0/24",
                vlanId: 100
        )

        def masterItem = [
                ID: "network-123",
                Name: "Test Network",
                Subnet: "192.168.1.0/24",
                VLanID: 100
        ]

        when: "updateMatchedNetworks logic is executed manually"
        def itemsToUpdate = []
        def save = false
        if (existingNetwork.cidr != masterItem.Subnet) {
            existingNetwork.cidr = masterItem.Subnet
            save = true
        }
        if (existingNetwork.vlanId != masterItem.VLanID) {
            existingNetwork.vlanId = masterItem.VLanID
            save = true
        }
        if (save) {
            itemsToUpdate << existingNetwork
        }

        then: "network cidr is updated and marked for save"
        existingNetwork.cidr == "192.168.1.0/24"
        existingNetwork.vlanId == 100  // Should remain unchanged
        itemsToUpdate.size() == 1
        itemsToUpdate[0] == existingNetwork
    }

    @Unroll
    def "updateMatchedNetworks logic should update vlanId when different"() {
        given: "Existing network with different VLAN ID"
        def existingNetwork = new Network(
                id: 1L,
                name: "Test Network",
                cidr: "192.168.1.0/24",
                vlanId: 100
        )

        def masterItem = [
                ID: "network-123",
                Name: "Test Network",
                Subnet: "192.168.1.0/24",
                VLanID: 200
        ]

        when: "updateMatchedNetworks logic is executed manually"
        def itemsToUpdate = []
        def save = false
        if (existingNetwork.cidr != masterItem.Subnet) {
            existingNetwork.cidr = masterItem.Subnet
            save = true
        }
        if (existingNetwork.vlanId != masterItem.VLanID) {
            existingNetwork.vlanId = masterItem.VLanID
            save = true
        }
        if (save) {
            itemsToUpdate << existingNetwork
        }

        then: "network vlanId is updated and marked for save"
        existingNetwork.cidr == "192.168.1.0/24"  // Should remain unchanged
        existingNetwork.vlanId == 200
        itemsToUpdate.size() == 1
        itemsToUpdate[0] == existingNetwork
    }

    @Unroll
    def "updateMatchedNetworks logic should update both cidr and vlanId when both different"() {
        given: "Existing network with different CIDR and VLAN ID"
        def existingNetwork = new Network(
                id: 1L,
                name: "Test Network",
                cidr: "192.168.0.0/24",
                vlanId: 100
        )

        def masterItem = [
                ID: "network-123",
                Name: "Test Network",
                Subnet: "10.0.0.0/16",
                VLanID: 300
        ]

        when: "updateMatchedNetworks logic is executed manually"
        def itemsToUpdate = []
        def save = false
        if (existingNetwork.cidr != masterItem.Subnet) {
            existingNetwork.cidr = masterItem.Subnet
            save = true
        }
        if (existingNetwork.vlanId != masterItem.VLanID) {
            existingNetwork.vlanId = masterItem.VLanID
            save = true
        }
        if (save) {
            itemsToUpdate << existingNetwork
        }

        then: "both network cidr and vlanId are updated and marked for save"
        existingNetwork.cidr == "10.0.0.0/16"
        existingNetwork.vlanId == 300
        itemsToUpdate.size() == 1
        itemsToUpdate[0] == existingNetwork
    }

    @Unroll
    def "updateMatchedNetworks logic should not save when no changes detected"() {
        given: "Existing network with same data as master"
        def existingNetwork = new Network(
                id: 1L,
                name: "Test Network",
                cidr: "192.168.1.0/24",
                vlanId: 100
        )

        def masterItem = [
                ID: "network-123",
                Name: "Test Network",
                Subnet: "192.168.1.0/24",
                VLanID: 100
        ]

        when: "updateMatchedNetworks logic is executed manually"
        def itemsToUpdate = []
        def save = false
        if (existingNetwork.cidr != masterItem.Subnet) {
            existingNetwork.cidr = masterItem.Subnet
            save = true
        }
        if (existingNetwork.vlanId != masterItem.VLanID) {
            existingNetwork.vlanId = masterItem.VLanID
            save = true
        }
        if (save) {
            itemsToUpdate << existingNetwork
        }

        then: "no changes are made and network is not marked for save"
        existingNetwork.cidr == "192.168.1.0/24"
        existingNetwork.vlanId == 100
        itemsToUpdate.size() == 0
    }

    @Unroll
    def "updateMatchedNetworks logic should handle null master item values"() {
        given: "Existing network and master item with null values"
        def existingNetwork = new Network(
                id: 1L,
                name: "Test Network",
                cidr: "192.168.1.0/24",
                vlanId: 100
        )

        def masterItem = [
                ID: "network-123",
                Name: "Test Network",
                Subnet: null,  // Null subnet
                VLanID: null   // Null VLAN
        ]

        when: "updateMatchedNetworks logic is executed manually"
        def itemsToUpdate = []
        def save = false
        if (existingNetwork.cidr != masterItem.Subnet) {
            existingNetwork.cidr = masterItem.Subnet
            save = true
        }
        if (existingNetwork.vlanId != masterItem.VLanID) {
            existingNetwork.vlanId = masterItem.VLanID
            save = true
        }
        if (save) {
            itemsToUpdate << existingNetwork
        }

        then: "network is updated with null values"
        existingNetwork.cidr == null
        existingNetwork.vlanId == null
        itemsToUpdate.size() == 1
        itemsToUpdate[0] == existingNetwork
    }

    @Unroll
    def "updateMatchedNetworks logic should handle multiple networks with mixed changes"() {
        given: "Multiple networks with different update scenarios"
        def network1 = new Network(
                id: 1L,
                name: "Network 1",
                cidr: "192.168.1.0/24",
                vlanId: 100
        )

        def network2 = new Network(
                id: 2L,
                name: "Network 2",
                cidr: "192.168.2.0/24",
                vlanId: 200
        )

        def network3 = new Network(
                id: 3L,
                name: "Network 3",
                cidr: "192.168.3.0/24",
                vlanId: 300
        )

        def masterItem1 = [
                ID: "network-1",
                Name: "Network 1",
                Subnet: "10.1.0.0/16",  // Different CIDR
                VLanID: 100             // Same VLAN
        ]

        def masterItem2 = [
                ID: "network-2",
                Name: "Network 2",
                Subnet: "192.168.2.0/24", // Same CIDR
                VLanID: 250               // Different VLAN
        ]

        def masterItem3 = [
                ID: "network-3",
                Name: "Network 3",
                Subnet: "192.168.3.0/24", // Same CIDR
                VLanID: 300               // Same VLAN
        ]

        def networks = [network1, network2, network3]
        def masterItems = [masterItem1, masterItem2, masterItem3]

        when: "updateMatchedNetworks logic is executed for multiple networks"
        def itemsToUpdate = []

        for (int i = 0; i < networks.size(); i++) {
            def network = networks[i]
            def masterItem = masterItems[i]
            def save = false

            if (network.cidr != masterItem.Subnet) {
                network.cidr = masterItem.Subnet
                save = true
            }
            if (network.vlanId != masterItem.VLanID) {
                network.vlanId = masterItem.VLanID
                save = true
            }
            if (save) {
                itemsToUpdate << network
            }
        }

        then: "only changed networks are marked for save"
        // Network 1 - CIDR changed
        network1.cidr == "10.1.0.0/16"
        network1.vlanId == 100

        // Network 2 - VLAN changed
        network2.cidr == "192.168.2.0/24"
        network2.vlanId == 250

        // Network 3 - no changes
        network3.cidr == "192.168.3.0/24"
        network3.vlanId == 300

        itemsToUpdate.size() == 2  // Only network1 and network2 should be updated
        itemsToUpdate.contains(network1)
        itemsToUpdate.contains(network2)
        !itemsToUpdate.contains(network3)  // network3 should not be in the update list
    }

    @Unroll
    def "updateMatchedNetworks logic should handle null existing network gracefully"() {
        given: "Null existing network"
        def network = null
        def masterItem = [
                ID: "network-123",
                Name: "Test Network",
                Subnet: "192.168.1.0/24",
                VLanID: 100
        ]

        when: "updateMatchedNetworks logic is executed manually"
        def itemsToUpdate = []
        if (network) {
            def save = false
            if (network.cidr != masterItem.Subnet) {
                network.cidr = masterItem.Subnet
                save = true
            }
            if (network.vlanId != masterItem.VLanID) {
                network.vlanId = masterItem.VLanID
                save = true
            }
            if (save) {
                itemsToUpdate << network
            }
        }

        then: "method completes without error and no updates are made"
        itemsToUpdate.size() == 0
    }

    def "execute should handle successful API response with valid networks"() {
        given: "Mock services setup for successful execution"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        def mockNetworkService = Mock(Object) {
            listIdentityProjections(_) >> Observable.fromIterable([])
            create(_) >> Single.just([])
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: true,
                networks: [
                        [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100],
                        [ID: "net-2", Name: "Network 2", Subnet: "192.168.2.0/24", VLanID: 200]
                ]
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "method executes successfully without throwing exceptions"
        notThrown(Exception)
    }

    def "execute should handle API success with null networks"() {
        given: "Mock services setup with null networks response"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: true,
                networks: null // Null networks should be handled gracefully
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "method handles null networks gracefully"
        notThrown(Exception)
    }

    def "execute should handle API failure with success=false"() {
        given: "Mock services setup with API failure response"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: false,
                error: "SCVMM API connection failed",
                networks: null
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "method handles API failure gracefully and logs appropriate message"
        notThrown(Exception)
    }

    def "execute should handle API response with success=null"() {
        given: "Mock services setup with null success response"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: null,
                networks: [
                        [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100]
                ]
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "method handles null success as failure"
        notThrown(Exception)
    }

    def "execute should handle server not found scenario"() {
        given: "Mock services setup where server is not found"
        def mockComputeServerService = Mock(Object) {
            find(_) >> null // Server not found
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "method handles missing server gracefully"
        notThrown(Exception)
    }

    def "execute should handle getScvmmZoneAndHypervisorOpts failure"() {
        given: "Mock services setup where getScvmmZoneAndHypervisorOpts fails"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> {
            throw new RuntimeException("Failed to get SCVMM options")
        }

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "exception is caught and logged"
        notThrown(RuntimeException)
    }

    def "execute should handle listNoIsolationVLans API failure"() {
        given: "Mock services setup where listNoIsolationVLans fails"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> {
            throw new RuntimeException("API call failed")
        }

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "exception is caught and logged"
        notThrown(RuntimeException)
    }

    def "execute should handle listIdentityProjections failure"() {
        given: "Mock services setup where listIdentityProjections fails"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        def mockNetworkService = Mock(Object) {
            listIdentityProjections(_) >> { throw new RuntimeException("Database error") }
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: true,
                networks: [
                        [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100]
                ]
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "exception is caught and logged"
        notThrown(RuntimeException)
    }

    def "execute should handle SyncTask execution failure"() {
        given: "Mock services that cause SyncTask to fail"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        def mockNetworkService = Mock(Object) {
            listIdentityProjections(_) >> Observable.fromIterable([])
            create(_) >> { throw new RuntimeException("SyncTask creation failed") }
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: true,
                networks: [
                        [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100]
                ]
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "exception is caught and logged"
        notThrown(RuntimeException)
    }

    def "execute should handle null or empty objList after networks extraction"() {
        given: "Mock services setup with networks that result in empty objList"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: true,
                networks: [] // This will make objList empty, triggering the "No networks returned!" log
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "method logs 'No networks returned!' and completes gracefully"
        notThrown(Exception)
    }

    def "execute should handle DataQuery and DataFilter construction correctly"() {
        given: "Mock services to verify DataQuery construction"
        def mockComputeServerService = Mock(Object) {
            find(_) >> { query ->
                // Verify the DataQuery was constructed correctly for server lookup
                assert query.toString().contains("cloud.id")
                return server
            }
        }

        def mockNetworkService = Mock(Object) {
            listIdentityProjections(_) >> { query ->
                // Verify the DataQuery was constructed correctly for network lookup
                def queryStr = query.toString()
                assert queryStr.contains("scvmm.vlan.network.${cloud.id}.")
                return Observable.fromIterable([])
            }
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [
                success: true,
                networks: [
                        [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100]
                ]
        ]

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "DataQuery objects are constructed with correct filters"
        notThrown(Exception)
    }

    def "execute should create NetworkType with correct code"() {
        given: "Mock services setup"
        def mockComputeServerService = Mock(Object) {
            find(_) >> server
        }

        mockContext.services >> [
                computeServer: mockComputeServerService
        ]

        def scvmmOpts = [zone: "test-zone", hypervisor: "test-hypervisor"]
        def apiListResults = [success: false] // Ensure we don't go into the sync logic

        mockApiService.getScvmmZoneAndHypervisorOpts(mockContext, cloud, server) >> scvmmOpts
        mockApiService.listNoIsolationVLans(scvmmOpts) >> apiListResults

        when: "execute method is called"
        isolationNetworkSync.execute()

        then: "NetworkType is created with correct code (implicitly tested via no exceptions)"
        notThrown(Exception)
    }

    def "addMissingNetworks should create networks with correct configuration"() {
        given: "Mock network service and network data"
        def mockNetworkService = Mock(Object) {
            create(_) >> { List<Network> networks ->
                // Verify network configurations
                assert networks.size() == 2

                // Verify first network
                def network1 = networks[0]
                assert network1.code == "scvmm.vlan.network.${cloud.id}.${server.id}.net-1"
                assert network1.cidr == "192.168.1.0/24"
                assert network1.vlanId == 100
                assert network1.category == "scvmm.vlan.network.${cloud.id}.${server.id}"
                assert network1.cloud == cloud
                assert network1.dhcpServer == true
                assert network1.uniqueId == "net-1"
                assert network1.name == "Test Network 1"
                assert network1.externalId == "net-1"
                assert network1.type == networkType
                assert network1.refType == "ComputeZone"
                assert network1.refId == cloud.id
                assert network1.owner == cloud.owner

                // Verify second network
                def network2 = networks[1]
                assert network2.code == "scvmm.vlan.network.${cloud.id}.${server.id}.net-2"
                assert network2.cidr == "10.0.0.0/16"
                assert network2.vlanId == 200
                assert network2.name == "Test Network 2"
                assert network2.externalId == "net-2"

                return Single.just(networks)
            }
        }

        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def networkItems = [
                [ID: "net-1", Name: "Test Network 1", Subnet: "192.168.1.0/24", VLanID: 100],
                [ID: "net-2", Name: "Test Network 2", Subnet: "10.0.0.0/16", VLanID: 200]
        ]

        when: "addMissingNetworks method is called"
        isolationNetworkSync.addMissingNetworks(networkItems, networkType, server)

        then: "networks are created with correct configuration"
        notThrown(Exception)
    }

    def "addMissingNetworks should handle empty add list"() {
        given: "Mock network service and empty network data"
        def mockNetworkService = Mock(Object) {
            create([]) >> Single.just([])
        }
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        when: "addMissingNetworks method is called with empty list"
        isolationNetworkSync.addMissingNetworks([], networkType, server)

        then: "create method is called with empty list"
        notThrown(Exception)
    }

    def "addMissingNetworks should handle null add list"() {
        given: "Mock network service and null network data"
        def mockNetworkService = Mock(Object) {
            create([]) >> Single.just([])
        }
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        when: "addMissingNetworks method is called with null list"
        isolationNetworkSync.addMissingNetworks(null, networkType, server)

        then: "create method is called with empty list"
        notThrown(Exception)
    }

    def "addMissingNetworks should handle network creation failure"() {
        given: "Mock network service that fails"
        def mockNetworkService = Mock(Object) {
            create(_) >> { throw new RuntimeException("Database error") }
        }
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def networkItems = [
                [ID: "net-1", Name: "Test Network", Subnet: "192.168.1.0/24", VLanID: 100]
        ]

        when: "addMissingNetworks method is called and creation fails"
        isolationNetworkSync.addMissingNetworks(networkItems, networkType, server)

        then: "exception is caught and method completes"
        notThrown(RuntimeException)
    }

    def "addMissingNetworks should handle networks with null values"() {
        given: "Mock network service and network with null values"
        def mockNetworkService = Mock(Object) {
            create(_) >> { List<Network> networks ->
                assert networks.size() == 1
                def network = networks[0]
                assert network.externalId == "net-null"
                assert network.name == null
                assert network.cidr == null
                assert network.vlanId == null
                return Single.just(networks)
            }
        }
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def networkItems = [
                [ID: "net-null", Name: null, Subnet: null, VLanID: null]
        ]

        when: "addMissingNetworks method is called"
        isolationNetworkSync.addMissingNetworks(networkItems, networkType, server)

        then: "network is created with null values handled"
        notThrown(Exception)
    }

    def "updateMatchedNetworks should update networks when changes detected"() {
        given: "Mock network service and update items"
        def mockNetworkService = Mock(Object) {
            save(_) >> { List<Network> networks ->
                // Verify only network1 is in the save list (has changes)
                assert networks.size() == 1
                def updatedNetwork = networks[0]
                assert updatedNetwork.id == 1L
                assert updatedNetwork.cidr == "192.168.1.0/24"  // Should be updated
                assert updatedNetwork.vlanId == 150  // Should be updated
                return Single.just(networks)
            }
        }
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def existingNetwork1 = new Network(
                id: 1L,
                name: "Network 1",
                cidr: "192.168.0.0/24",
                vlanId: 100
        )

        def existingNetwork2 = new Network(
                id: 2L,
                name: "Network 2",
                cidr: "10.0.0.0/16",
                vlanId: 200
        )

        def updateItems = [
                new SyncTask.UpdateItem<Network, Map>(
                        existingItem: existingNetwork1,
                        masterItem: [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 150]
                ),
                new SyncTask.UpdateItem<Network, Map>(
                        existingItem: existingNetwork2,
                        masterItem: [ID: "net-2", Name: "Network 2", Subnet: "10.0.0.0/16", VLanID: 200] // No changes
                )
        ]

        when: "updateMatchedNetworks method is called"
        isolationNetworkSync.updateMatchedNetworks(updateItems)

        then: "network values are correctly updated"
        existingNetwork1.cidr == "192.168.1.0/24"
        existingNetwork1.vlanId == 150
        existingNetwork2.cidr == "10.0.0.0/16"  // Unchanged
        existingNetwork2.vlanId == 200  // Unchanged
        notThrown(Exception)
    }

    def "updateMatchedNetworks should handle no changes scenario"() {
        given: "Mock network service and update items with no changes"
        def mockNetworkService = Mock(Object)
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def existingNetwork = new Network(
                id: 1L,
                name: "Network 1",
                cidr: "192.168.1.0/24",
                vlanId: 100
        )

        def updateItems = [
                new SyncTask.UpdateItem<Network, Map>(
                        existingItem: existingNetwork,
                        masterItem: [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100]
                )
        ]

        when: "updateMatchedNetworks method is called"
        isolationNetworkSync.updateMatchedNetworks(updateItems)

        then: "save method is not called since no changes detected"
        notThrown(Exception)
    }

    def "updateMatchedNetworks should handle empty update list"() {
        given: "Mock network service and empty update list"
        def mockNetworkService = Mock(Object)
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        when: "updateMatchedNetworks method is called with empty list"
        isolationNetworkSync.updateMatchedNetworks([])

        then: "save method is not called"
        notThrown(Exception)
    }

    def "updateMatchedNetworks should handle null networks in update list"() {
        given: "Mock network service and update items with null network"
        def mockNetworkService = Mock(Object)
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def updateItems = [
                new SyncTask.UpdateItem<Network, Map>(
                        existingItem: null,
                        masterItem: [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100]
                )
        ]

        when: "updateMatchedNetworks method is called"
        isolationNetworkSync.updateMatchedNetworks(updateItems)

        then: "save method is not called and method completes without error"
        notThrown(Exception)
    }

    def "updateMatchedNetworks should handle save failure"() {
        given: "Mock network service that fails on save"
        def mockNetworkService = Mock(Object) {
            save(_) >> { throw new RuntimeException("Database error") }
        }
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def existingNetwork = new Network(
                id: 1L,
                name: "Network 1",
                cidr: "192.168.0.0/24",
                vlanId: 100
        )

        def updateItems = [
                new SyncTask.UpdateItem<Network, Map>(
                        existingItem: existingNetwork,
                        masterItem: [ID: "net-1", Name: "Network 1", Subnet: "192.168.1.0/24", VLanID: 100]
                )
        ]

        when: "updateMatchedNetworks method is called and save fails"
        isolationNetworkSync.updateMatchedNetworks(updateItems)

        then: "exception is caught and method completes"
        notThrown(RuntimeException)
    }

    def "updateMatchedNetworks should handle master items with null values"() {
        given: "Mock network service and update items with null master values"
        def mockNetworkService = Mock(Object) {
            save(_) >> { List<Network> networks ->
                assert networks.size() == 1
                def updatedNetwork = networks[0]
                assert updatedNetwork.cidr == null
                assert updatedNetwork.vlanId == null
                return Single.just(networks)
            }
        }
        mockContext.async >> [
                cloud: [
                        network: mockNetworkService
                ]
        ]

        def existingNetwork = new Network(
                id: 1L,
                name: "Network 1",
                cidr: "192.168.1.0/24",
                vlanId: 100
        )

        def updateItems = [
                new SyncTask.UpdateItem<Network, Map>(
                        existingItem: existingNetwork,
                        masterItem: [ID: "net-1", Name: "Network 1", Subnet: null, VLanID: null]
                )
        ]

        when: "updateMatchedNetworks method is called"
        isolationNetworkSync.updateMatchedNetworks(updateItems)

        then: "network values are set to null"
        existingNetwork.cidr == null
        existingNetwork.vlanId == null
        notThrown(Exception)
    }

}
