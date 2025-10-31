package com.morpheusdata.scvmm

import com.morpheusdata.scvmm.helper.morpheus.types.StorageVolumeTypeHelper
import com.morpheusdata.scvmm.logging.LogInterface
import com.morpheusdata.scvmm.logging.LogWrapper
import com.morpheusdata.scvmm.sync.CloudCapabilityProfilesSync
import com.morpheusdata.scvmm.sync.ClustersSync
import com.morpheusdata.scvmm.sync.DatastoresSync
import com.morpheusdata.scvmm.sync.HostSync
import com.morpheusdata.scvmm.sync.IpPoolsSync
import com.morpheusdata.scvmm.sync.IsolationNetworkSync
import com.morpheusdata.scvmm.sync.RegisteredStorageFileSharesSync
import com.morpheusdata.scvmm.sync.NetworkSync
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.providers.ProvisionProvider
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.MorpheusUtils
import com.morpheusdata.model.*
import com.morpheusdata.request.ValidateCloudRequest
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.scvmm.sync.TemplatesSync
import com.morpheusdata.scvmm.sync.VirtualMachineSync
import groovy.util.logging.Slf4j

class ScvmmCloudProvider implements CloudProvider {
	public static final String CLOUD_PROVIDER_CODE = 'scvmm'
	private static final String ZONE_TYPE_SCVMM = 'zoneType.scvmm'
	private static final String CONFIG_CONTEXT = 'config'
	private static final String SCVMM_SOURCE_TYPE = 'scvmm'
	private static final String SCVMM_HOST_FIELD = 'SCVMM Host'
	private static final String CREDENTIALS_FIELD = 'Credentials'
	private static final String USERNAME_FIELD = 'Username'
	private static final String PASSWORD_FIELD = 'Password'
	private static final String CLOUD_FIELD = 'Cloud'
	private static final String HOST_GROUP_FIELD = 'Host Group'
	private static final String CLUSTER_FIELD = 'Cluster'
	private static final String HOST_CONFIG_PROPERTY = 'host'
	private static final String LIBRARY_SHARE_FIELD = 'Library Share'
	private static final String SCVMM_CIRCULAR_SVG = 'scvmm-circular.svg'
	private static final String SHARED_CONTROLLER_FIELD = 'Shared Controller'
	private static final String WORKING_PATH_FIELD = 'Working Path'
	private static final String DISK_PATH_FIELD = 'Disk Path'
	private static final String HIDE_HOST_SELECTION_FIELD = 'Hide Host Selection From Users'
	private static final String INVENTORY_EXISTING_FIELD = 'Inventory Existing Instances'
	private static final String ENABLE_HYPERVISOR_CONSOLE_FIELD = 'Enable Hypervisor Console'
	private static final String INSTALL_AGENT_CODE = 'gomorpheus.label.installAgent'
	private static final String INSTALL_AGENT_FIELD = 'Install Agent'
	private static final String CREDENTIAL_DEPENDS_ON =
			'config.host, config.username, config.password, credential.type, credential.username, credential.password'
	private static final int DISPLAY_ORDER_INCREMENT = 10
	private static final String SHARED_CONTROLLER_REQUIRED_MSG = 'You must specify a shared controller'
	private static final String SCVMM_CONTROLLER_CODE = 'scvmmController'
	private static final String ZONE_ID_FIELD = 'zone.id'
	private static final String COMPUTE_SERVER_TYPE_CODE_FIELD = 'computeServerType.code'
	// Add these constants to the existing ones at the top of the class
	private static final String SERVER_TYPE_HYPERVISOR_FIELD = 'serverType'
	private static final String SERVER_TYPE_HYPERVISOR_VALUE = 'hypervisor'
	private static final String SHARED_CONTROLLER_ERROR_KEY = 'sharedController'

	// Add these constants to the existing ones at the top of the class
	private static final String SCVMM_COMPUTE_SERVICE = 'scvmmComputeService'
	private static final String SCVMM_PROVISION_TYPE = 'scvmm'
	private static final String SCVMM_HYPERVISOR_PROVISION_TYPE = 'scvmm-hypervisor'
	private static final String SCVMM_INSTANCE_NAME = 'SCVMM Instance'
	private static final String MORPHEUS_SCVMM_NODE = 'morpheus-scvmm-node'
	private static final String DOCKER_ENGINE = 'docker'
	private static final String EMPTY_DESCRIPTION = ''
	private static final String UNMANAGED_NODE_TYPE = 'unmanaged'


	protected MorpheusContext context
	protected ScvmmPlugin plugin
	ScvmmApiService apiService
	private LogInterface log = LogWrapper.instance

	ScvmmCloudProvider(ScvmmPlugin plugin, MorpheusContext context) {
		super()
		this.@plugin = plugin
		this.@context = context
		this.apiService = new ScvmmApiService(context)
	}

	/**
	 * Grabs the description for the CloudProvider
	 * @return String
	 */
	@Override
	String getDescription() {
		return 'System Center Virtual Machine Manager'
	}

	/**
	 * Returns the Cloud logo for display when a user needs to view or add this cloud. SVGs are preferred.
	 * @since 0.13.0
	 * @return Icon representation of assets stored in the src/assets of the project.
	 */
	@Override
	Icon getIcon() {
		return new Icon(path:'scvmm.svg', darkPath:'scvmm-dark.svg')
	}

	/**
	 * Returns the circular Cloud logo for display when a user needs to view or add this cloud. SVGs are preferred.
	 * @since 0.13.6
	 * @return Icon
	 */
	@Override
	Icon getCircularIcon() {
		// TODO: change icon paths to correct filenames once added to your project
		return new Icon(path: SCVMM_CIRCULAR_SVG, darkPath: SCVMM_CIRCULAR_SVG)
	}


	/**
	 * Provides a Collection of OptionType inputs that define the required input fields for defining a cloud integration
	 * @return Collection of OptionType
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		def displayOrder = 0
		Collection<OptionType> options = []

		options << new OptionType(
				name: SCVMM_HOST_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.host',
				fieldName: HOST_CONFIG_PROPERTY,
				displayOrder: displayOrder,
				fieldCode: 'gomorpheus.scvmm.option.host',
				fieldLabel: SCVMM_HOST_FIELD,
				required: true,
				inputType: OptionType.InputType.TEXT,
				fieldContext: CONFIG_CONTEXT,
		)

		options << new OptionType(
				name: CREDENTIALS_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.credential',
				fieldName: 'type',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.label.credentials',
				fieldLabel: CREDENTIALS_FIELD,
				required: true,
				defaultValue:'local',
				inputType: OptionType.InputType.CREDENTIAL,
				fieldContext: 'credential',
				optionSource:'credentials',
				config: '{"credentialTypes":["username-password"]}'
		)

		options << new OptionType(
				name: USERNAME_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.username',
				fieldName: 'username',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.Username',
				fieldLabel: USERNAME_FIELD,
				required: true,
				inputType: OptionType.InputType.TEXT,
				fieldContext: CONFIG_CONTEXT,
				localCredential: true
		)

		options << new OptionType(
				name: PASSWORD_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.password',
				fieldName: 'password',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.Password',
				fieldLabel: PASSWORD_FIELD,
				required: true,
				inputType: OptionType.InputType.PASSWORD,
				fieldContext: CONFIG_CONTEXT,
				localCredential: true
		)

		options << new OptionType(
				name: CLOUD_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.cloud',
				fieldName: 'regionCode',
				optionSourceType: SCVMM_SOURCE_TYPE,
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.Cloud',
				fieldLabel: CLOUD_FIELD,
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmCloud',
				fieldContext:'domain',
				noBlank: true,
				dependsOn: CREDENTIAL_DEPENDS_ON
		)

		options << new OptionType(
				name: HOST_GROUP_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.hostGroup',
				fieldName: 'hostGroup',
				optionSourceType: SCVMM_SOURCE_TYPE,
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.HostGroup',
				fieldLabel: HOST_GROUP_FIELD,
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmHostGroup',
				fieldContext: CONFIG_CONTEXT,
				noBlank: true,
				dependsOn: CREDENTIAL_DEPENDS_ON,
		)

		options << new OptionType(
				name: CLUSTER_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.Cluster',
				fieldName: 'cluster',
				optionSourceType: SCVMM_SOURCE_TYPE,
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.Cluster',
				fieldLabel: CLUSTER_FIELD,
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmCluster',
				fieldContext: CONFIG_CONTEXT,
				noBlank: true,
				dependsOn: 'config.host, config.username, config.password, config.hostGroup, ' +
						'credential.type, credential.username, credential.password'
		)

		options << new OptionType(
				name: LIBRARY_SHARE_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.libraryShare',
				fieldName: 'libraryShare',
				optionSourceType: SCVMM_SOURCE_TYPE,
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.LibraryShare',
				fieldLabel: LIBRARY_SHARE_FIELD,
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmLibraryShares',
				fieldContext: CONFIG_CONTEXT,
				noBlank: true,
				dependsOn: CREDENTIAL_DEPENDS_ON
		)

		options << new OptionType(
				name: SHARED_CONTROLLER_FIELD,
				category: ZONE_TYPE_SCVMM,
				code: 'zoneType.scvmm.sharedController',
				fieldName: SHARED_CONTROLLER_ERROR_KEY,
				optionSourceType: SCVMM_SOURCE_TYPE,
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.SharedController',
				fieldLabel:SHARED_CONTROLLER_FIELD,
				required: false,
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmSharedControllers',
				fieldContext: CONFIG_CONTEXT,
				editable: false
		)

		options << new OptionType(
				name: WORKING_PATH_FIELD,
				code: 'zoneType.scvmm.workingPath',
				fieldName: 'workingPath',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.WorkingPath',
				fieldLabel:WORKING_PATH_FIELD,
				required: true,
				inputType: OptionType.InputType.TEXT,
				defaultValue: 'c:\\Temp'
		)

		options << new OptionType(
				name: DISK_PATH_FIELD,
				code: 'zoneType.scvmm.diskPath',
				fieldName: 'diskPath',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldCode: 'gomorpheus.optiontype.DiskPath',
				fieldLabel: DISK_PATH_FIELD,
				required: true,
				inputType: OptionType.InputType.TEXT,
				defaultValue:'c:\\VirtualDisks'
		)

		options << new OptionType(
				name: HIDE_HOST_SELECTION_FIELD,
				code: 'zoneType.scvmm.hideHostSelection',
				fieldName: 'HideHostSelectionFromUsers',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldLabel: HIDE_HOST_SELECTION_FIELD,
				required: false,
				inputType: OptionType.InputType.CHECKBOX,
				fieldContext: CONFIG_CONTEXT,
		)

		options << new OptionType(
				name: INVENTORY_EXISTING_FIELD,
				code: 'zoneType.scvmm.importExisting',
				fieldName: 'importExisting',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldLabel: INVENTORY_EXISTING_FIELD,
				required: false,
				inputType: OptionType.InputType.CHECKBOX,
				fieldContext: CONFIG_CONTEXT,
		)

		options << new OptionType(
				name: ENABLE_HYPERVISOR_CONSOLE_FIELD,
				code: 'zoneType.scvmm.enableHypervisorConsole',
				fieldName: 'enableHypervisorConsole',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				fieldLabel: ENABLE_HYPERVISOR_CONSOLE_FIELD,
				required: false,
				inputType: OptionType.InputType.CHECKBOX,
				fieldContext: CONFIG_CONTEXT,
		)

		options << new OptionType(
				name: INSTALL_AGENT_FIELD,
				code: INSTALL_AGENT_CODE,
				inputType: OptionType.InputType.CHECKBOX,
				fieldName: 'installAgent',
				fieldContext: CONFIG_CONTEXT,
				fieldCode: INSTALL_AGENT_CODE,
				fieldLabel: INSTALL_AGENT_FIELD,
				fieldGroup: 'Advancedend',
				displayOrder: displayOrder += DISPLAY_ORDER_INCREMENT,
				required: false,
				enabled: true,
				editable: false,
				global: false,
				custom: true,
		)

		return options
	}

	/**
	 * Grabs available provisioning providers related to the target Cloud Plugin. Some clouds have multiple provisioning
	 * providers or some clouds allow for service based providers on top like (Docker or Kubernetes).
	 * @return Collection of ProvisionProvider
	 */
	@Override
	Collection<ProvisionProvider> getAvailableProvisionProviders() {
		return this.@plugin.getProvidersByType(ProvisionProvider) as Collection<ProvisionProvider>
	}

	/**
	 * Grabs available backup providers related to the target Cloud Plugin.
	 * @return Collection of BackupProvider
	 */
	@Override
	Collection<BackupProvider> getAvailableBackupProviders() {
		Collection<BackupProvider> providers = []
		return providers
	}

	/**
	 * Provides a Collection of {@link NetworkType} related to this CloudProvider
	 * @return Collection of NetworkType
	 */
	@Override
	Collection<NetworkType> getNetworkTypes() {
		Collection<NetworkType> networks = []
		return networks
	}

	/**
	 * Provides a Collection of {@link NetworkSubnetType} related to this CloudProvider
	 * @return Collection of NetworkSubnetType
	 */
	@Override
	Collection<NetworkSubnetType> getSubnetTypes() {
		Collection<NetworkSubnetType> subnets = []
		return subnets
	}

	/**
	 * Provides a Collection of {@link StorageVolumeType} related to this CloudProvider
	 * @return Collection of StorageVolumeType
	 */
	@Override
	Collection<StorageVolumeType> getStorageVolumeTypes() {
		return StorageVolumeTypeHelper.getAllStorageVolumeTypes()
	}

	/**
	 * Provides a Collection of {@link StorageControllerType} related to this CloudProvider
	 * @return Collection of StorageControllerType
	 */
	@Override
	Collection<StorageControllerType> getStorageControllerTypes() {
		Collection<StorageControllerType> controllerTypes = []
		return controllerTypes
	}

	/**
	 * Create the SSH options.
	 *
	 * @return The SSH options.
	 */
	protected static Collection<OptionType> createSshOptions() {
		Collection<OptionType> sshOptions = []
		sshOptions << new OptionType(code: 'computeServerType.global.sshHost')
		sshOptions << new OptionType(code: 'computeServerType.global.sshPort')
		sshOptions << new OptionType(code: 'computeServerType.global.sshUsername')
		sshOptions << new OptionType(code: 'computeServerType.global.sshPassword')
		return sshOptions
	}

	/**
	 * Grabs all {@link ComputeServerType} objects that this CloudProvider can represent during
	 * a sync or during a provision.
	 * @return collection of ComputeServerType
	 */
	@Override
	Collection<ComputeServerType> getComputeServerTypes() {
		Collection<ComputeServerType> serverTypes = []
		Collection<OptionType> sshOptions = createSshOptions()

		// Host option type is used by multiple compute server types.
		OptionType hostOptionType = new OptionType(code:'computeServerType.scvmm.capabilityProfile', inputType: OptionType.InputType.SELECT,
				name:'capability profile', category:'provisionType.scvmm', optionSourceType:SCVMM_SOURCE_TYPE, fieldName:'scvmmCapabilityProfile',
				fieldCode: 'gomorpheus.optiontype.CapabilityProfile', fieldLabel:'Capability Profile', fieldContext:CONFIG_CONTEXT, fieldGroup:'Options',
				required:true, enabled:true, optionSource:'scvmmCapabilityProfile', editable:true, global:false, placeHolder:null, helpBlock:'',
				defaultValue:null, custom:false, displayOrder:10, fieldClass:null
		)

		serverTypes << new ComputeServerType( code: UNMANAGED_NODE_TYPE, name: 'Linux VM', description: 'vm', platform: PlatformType.linux, nodeType: UNMANAGED_NODE_TYPE,
				enabled: true, selectable: false, externalDelete: false, managed: false, controlPower: false, controlSuspend: false, creatable: true,
				computeService: 'unmanagedComputeService', displayOrder: 100, hasAutomation: false, containerHypervisor: false, bareMetalHost: false,
				vmHypervisor: false, agentType: null, managedServerType: 'managed', guestVm: true, provisionTypeCode: UNMANAGED_NODE_TYPE, optionTypes: sshOptions
		)

		//scvmm
		serverTypes << new ComputeServerType(code:SCVMM_CONTROLLER_CODE, name:'SCVMM Manager', description:EMPTY_DESCRIPTION, platform:PlatformType.windows,
				nodeType:MORPHEUS_SCVMM_NODE, enabled:true, selectable:false, externalDelete:false, managed:false, controlPower:false,
				controlSuspend:false, creatable:false, computeService:SCVMM_COMPUTE_SERVICE, displayOrder:0, hasAutomation:false, containerHypervisor:false,
				bareMetalHost:false, vmHypervisor:true, agentType: ComputeServerType.AgentType.node, provisionTypeCode:SCVMM_HYPERVISOR_PROVISION_TYPE
		)

		//vms
		serverTypes << new ComputeServerType(code:'scvmmHypervisor', name:'SCVMM Hypervisor', description:EMPTY_DESCRIPTION, platform:PlatformType.windows,
				nodeType:MORPHEUS_SCVMM_NODE, enabled:true, selectable:false, externalDelete:false, managed:false, controlPower:false,
				controlSuspend:false, creatable:false, computeService:SCVMM_COMPUTE_SERVICE, displayOrder:0, hasAutomation:false, containerHypervisor:false,
				bareMetalHost:false, vmHypervisor:true, agentType: ComputeServerType.AgentType.node, provisionTypeCode:SCVMM_HYPERVISOR_PROVISION_TYPE
		)
		serverTypes << new ComputeServerType(code:'scvmmWindows', name:'SCVMM Windows Node', description:EMPTY_DESCRIPTION, platform:PlatformType.windows,
				nodeType:'morpheus-windows-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true,
				controlSuspend:false, creatable:false, computeService:SCVMM_COMPUTE_SERVICE, displayOrder:7, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:false, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.node, guestVm:true,
				provisionTypeCode:SCVMM_PROVISION_TYPE
		)
		serverTypes << new ComputeServerType(code:'scvmmVm', name:SCVMM_INSTANCE_NAME, description:EMPTY_DESCRIPTION, platform:PlatformType.linux,
				nodeType:'morpheus-vm-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true, controlSuspend:false,
				creatable:false, computeService:SCVMM_COMPUTE_SERVICE, displayOrder: 0, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:false, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.guest, guestVm:true,
				provisionTypeCode:SCVMM_PROVISION_TYPE
		)
		//windows container host - not used
		serverTypes << new ComputeServerType(code:'scvmmWindowsVm', name:'SCVMM Windows Instance', description:EMPTY_DESCRIPTION, platform:PlatformType.windows,
				nodeType:'morpheus-windows-vm-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true,
				controlSuspend:false, creatable:false, computeService:SCVMM_COMPUTE_SERVICE, displayOrder: 0, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:false, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.guest, guestVm:true,
				provisionTypeCode:SCVMM_PROVISION_TYPE
		)
		serverTypes << new ComputeServerType(code:'scvmmUnmanaged', name:SCVMM_INSTANCE_NAME, description:'scvmm vm', platform:PlatformType.linux,
				nodeType:UNMANAGED_NODE_TYPE, enabled:true, selectable:false, externalDelete:true, managed:false, controlPower:true, controlSuspend:false,
				creatable:false, computeService:SCVMM_COMPUTE_SERVICE, displayOrder:99, hasAutomation:false, containerHypervisor:false,
				bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.guest, managedServerType:'scvmmVm', guestVm:true,
				provisionTypeCode:SCVMM_PROVISION_TYPE
		)

		//docker
		serverTypes << new ComputeServerType(code:'scvmmLinux', name:'SCVMM Docker Host', description:EMPTY_DESCRIPTION, platform:PlatformType.linux,
				nodeType:'morpheus-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true, controlSuspend:false,
				creatable:false, computeService:SCVMM_COMPUTE_SERVICE, displayOrder: 6, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:true, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.node, containerEngine:DOCKER_ENGINE,
				provisionTypeCode:SCVMM_PROVISION_TYPE, computeTypeCode:'docker-host', optionTypes:[hostOptionType]
		)

		//kubernetes
		serverTypes << new ComputeServerType(code:'scvmmKubeMaster', name:'SCVMM Kubernetes Master', description:EMPTY_DESCRIPTION, platform:PlatformType.linux,
				nodeType:'kube-master', hasMaintenanceMode: true, reconfigureSupported: true, enabled:true, selectable:false, externalDelete:true, managed:true,
				controlPower:true, controlSuspend:true, creatable:true, supportsConsoleKeymap: true, computeService:SCVMM_COMPUTE_SERVICE,
				displayOrder:10, hasAutomation:true, containerHypervisor:true, bareMetalHost:false, vmHypervisor:false,
				agentType:ComputeServerType.AgentType.host, containerEngine:DOCKER_ENGINE, provisionTypeCode:SCVMM_PROVISION_TYPE, computeTypeCode:'kube-master',
				optionTypes:[hostOptionType]
		)
		serverTypes << new ComputeServerType(code:'scvmmKubeWorker', name:'SCVMM Kubernetes Worker', description:EMPTY_DESCRIPTION, platform:PlatformType.linux,
				nodeType:'kube-worker', hasMaintenanceMode: true, reconfigureSupported: true, enabled:true, selectable:false, externalDelete:true, managed:true,
				controlPower:true, controlSuspend:true, creatable:true, supportsConsoleKeymap: true, computeService:SCVMM_COMPUTE_SERVICE,
				displayOrder:10, hasAutomation:true, containerHypervisor:true, bareMetalHost:false, vmHypervisor:false,
				agentType:ComputeServerType.AgentType.guest, containerEngine:DOCKER_ENGINE, provisionTypeCode:SCVMM_PROVISION_TYPE, computeTypeCode:'kube-worker',
				optionTypes:[hostOptionType]
		)

		return serverTypes
	}

	/**
	 * Validates the submitted cloud information to make sure it is functioning correctly.
	 * If a {@link ServiceResponse} is not marked as successful then the validation results will be
	 * bubbled up to the user.
	 * @param cloudInfo cloud
	 * @param validateCloudRequest Additional validation information
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse validate(Cloud cloudInfo, ValidateCloudRequest validateCloudRequest) {
		log.debug("validate cloud: {}", cloudInfo)
		def rtn = [success: false, zone: cloudInfo, errors: [:]]
		try {
			if (rtn.zone) {
				def requiredFields = [HOST_CONFIG_PROPERTY, 'workingPath', 'diskPath', 'libraryShare']
				def scvmmOpts = apiService.getScvmmZoneOpts(context, cloudInfo)
				def zoneConfig = cloudInfo.getConfigMap()
				rtn.errors = validateRequiredConfigFields(requiredFields, zoneConfig)
				// Verify that a shared controller is selected if we already have an scvmm zone
				// pointed to this host (MUST SHARE THE CONTROLLER)
				def validateControllerResults = validateSharedController(cloudInfo)
				if (!validateControllerResults.success) {
					rtn.errors[SHARED_CONTROLLER_ERROR_KEY] = validateControllerResults.msg ?: SHARED_CONTROLLER_REQUIRED_MSG
				}
				if (!validateCloudRequest?.credentialUsername) {
					rtn.msg = 'Enter a username'
					rtn.errors.username = 'Enter a username'
				} else if (!validateCloudRequest?.credentialPassword) {
					rtn.msg = 'Enter a password'
					rtn.errors.password = 'Enter a password'
				}
				if (rtn.errors.size() == 0) {
					//set install agent
					def installAgent = MorpheusUtils.parseBooleanConfig(zoneConfig.installAgent)
					cloudInfo.setConfigProperty('installAgent', installAgent)
					//build opts
					scvmmOpts += [
							hypervisor : [:],
							sshHost    : zoneConfig.host,
							sshUsername: validateCloudRequest?.credentialUsername,
							sshPassword: validateCloudRequest?.credentialPassword,
							zoneRoot   : zoneConfig.workingPath,
							diskRoot   : zoneConfig.diskPath
					]
					def vmSitches = apiService.listClouds(scvmmOpts)
					log.debug("vmSitches: ${vmSitches}")
					if (vmSitches.success == true)
						rtn.success = true
					if (rtn.success == false)
						rtn.msg = 'Error connecting to scvmm'
				}
			} else {
				rtn.message = 'No zone found'
			}
		} catch (e) {
			log.error("An Exception Has Occurred", e)
		}
		return ServiceResponse.create(rtn)
	}

	/**
	 * Called when a Cloud From Morpheus is first saved. This is a hook provided to take care of initial state
	 * assignment that may need to take place.
	 * @param cloudInfo instance of the cloud object that is being initialized.
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse initializeCloud(Cloud cloudInfo) {
		log.debug ('initializing cloud: {}', cloudInfo.code)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			if(cloudInfo) {
				if(cloudInfo.enabled == true) {
					def initResults = initializeHypervisor(cloudInfo)
					log.debug("initResults: {}", initResults)
					if(initResults.success == true) {
						refresh(cloudInfo)
					}
					rtn.success = true
				}
			} else {
				rtn.msg = 'No zone found'
			}
		} catch(e) {
			log.error("initialize cloud error: {}",e)
		}
		return rtn
	}

	def initializeHypervisor(cloud) {
		def rtn = [success: false]
		log.debug("cloud: ${cloud}")
		def sharedController = cloud.getConfigProperty(SHARED_CONTROLLER_ERROR_KEY)
		if(sharedController) {
			// No controller needed.. we are sharing another cloud's controller
			rtn.success = true
		} else {
			ComputeServer newServer
			def opts = apiService.getScvmmInitializationOpts(cloud)
			def serverInfo = apiService.getScvmmServerInfo(opts)
			String versionCode
			versionCode = apiService.extractWindowsServerVersion(serverInfo.osName)
			if(serverInfo.success == true && serverInfo.hostname) {
				newServer = context.services.computeServer.find(new DataQuery().withFilters(
						new DataFilter(ZONE_ID_FIELD, cloud.id),
						new DataOrFilter(
								new DataFilter('hostname', serverInfo.hostname),
								new DataFilter('name', serverInfo.hostname)
						)
				))
				if(!newServer) {
					newServer = new ComputeServer()
					newServer.account = cloud.account
					newServer.cloud = cloud
					newServer.computeServerType = context.async.cloud.findComputeServerTypeByCode(SCVMM_CONTROLLER_CODE).blockingGet()
					// Create proper OS code format
					newServer.serverOs = new OsType(code: versionCode)
					newServer.name = serverInfo.hostname
					newServer = context.services.computeServer.create(newServer)
				}
				if(serverInfo.hostname) {
					newServer.hostname = serverInfo.hostname
				}
				newServer.sshHost = cloud.getConfigProperty(HOST_CONFIG_PROPERTY)
				newServer.internalIp = newServer.sshHost
				newServer.externalIp = newServer.sshHost
				newServer.sshUsername = apiService.getUsername(cloud)
				newServer.sshPassword = apiService.getPassword(cloud)
				newServer.setConfigProperty('workingPath', cloud.getConfigProperty('workingPath'))
				newServer.setConfigProperty('diskPath', cloud.getConfigProperty('diskPath'))
			}

			def maxStorage = getMaxStorage(serverInfo)
			def maxMemory = getMaxMemory(serverInfo)
			def maxCores = 1
			newServer.serverOs = context.async.osType.find(new DataQuery().withFilter('code', versionCode)).blockingGet()
			newServer.platform = 'windows'
			def tokens = versionCode.split("\\.")
			def version = tokens.length > 2 ? tokens[2] : ""
			if (newServer.serverOs == null) {
				newServer.serverOs = context.async.osType.find(new DataQuery().withFilter('code', "windows.server.${version}")).blockingGet()
			}
			newServer.platformVersion = version
			newServer.statusDate = new Date()
			newServer.status = 'provisioning'
			newServer.powerState = 'on'
			newServer.serverType = SERVER_TYPE_HYPERVISOR_VALUE
			newServer.osType = 'windows' //linux, windows, unmanaged
			newServer.maxMemory = maxMemory
			newServer.maxCores = maxCores
			newServer.maxStorage = maxStorage

			// initializeHypervisor from context
			log.debug("newServer: ${newServer}")
			context.services.computeServer.save(newServer)
			if(newServer) {
				context.async.hypervisorService.initialize(newServer)
				rtn.success = true
			}
		}
		return rtn
	}

	/**
	 * Zones/Clouds are refreshed periodically by the Morpheus Environment. This includes things like caching of brownfield
	 * environments and resources such as Networks, Datastores, Resource Pools, etc.
	 * @param cloudInfo cloud
	 * @return ServiceResponse. If ServiceResponse.success == true, then Cloud status will be set to Cloud.Status.ok. If
	 * ServiceResponse.success == false, the Cloud status will be set to ServiceResponse.data['status'] or Cloud.Status.error
	 * if not specified. So, to indicate that the Cloud is offline, return `ServiceResponse.error('cloud is not reachable', null, [status: Cloud.Status.offline])`
	 */
	@Override
	ServiceResponse refresh(Cloud cloudInfo) {
		log.debug("refresh: {}", cloudInfo)
		ServiceResponse response = ServiceResponse.prepare()
		try {
			def syncDate = new Date()
			//why would we ever have more than 1? i don't think we would
			def scvmmController = getScvmmController(cloudInfo)
			if (scvmmController) {
				def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloudInfo, scvmmController)
				def hostOnline = ConnectionUtils.testHostConnectivity(scvmmOpts.sshHost, 5985, false, true, null)
				log.debug("hostOnline: {}", hostOnline)
				if (hostOnline) {
					def checkResults = checkCommunication(cloudInfo, scvmmController)
					if (checkResults.success == true) {
						updateHypervisorStatus(scvmmController, 'provisioned', 'on', '')
						//updateZoneStatus(zone, 'syncing', null)
						context.async.cloud.updateCloudStatus(cloudInfo, Cloud.Status.syncing, null, syncDate)

						def now = new Date().time
						new NetworkSync(context, cloudInfo).execute()
						log.debug("${cloudInfo.name}: NetworkSync in ${new Date().time - now}ms")

						now = new Date().time
						new ClustersSync(context, cloudInfo).execute()
						log.debug("${cloudInfo.name}: ClustersSync in ${new Date().time - now}ms")

						now = new Date().time
						new IsolationNetworkSync(context, cloudInfo, apiService).execute()
						log.debug("${cloudInfo.name}: IsolationNetworkSync in ${new Date().time - now}ms")

						now = new Date().time
						new HostSync(cloudInfo, scvmmController, context).execute()
						log.debug("${cloudInfo.name}: HostSync in ${new Date().time - now}ms")

						now = new Date().time
						new DatastoresSync(scvmmController, cloudInfo, context).execute()
						log.debug("${cloudInfo.name}: DatastoresSync in ${new Date().time - now}ms")

						now = new Date().time
						new RegisteredStorageFileSharesSync(cloudInfo, scvmmController, context).execute()
						log.debug("${cloudInfo.name}: RegisteredStorageFileSharesSync in ${new Date().time - now}ms")

						now = new Date().time
						new CloudCapabilityProfilesSync(context, cloudInfo).execute()
						log.debug("${cloudInfo.name}: CloudCapabilityProfilesSync in ${new Date().time - now}ms")

						now = new Date().time
						new TemplatesSync(cloudInfo, scvmmController, context, this).execute()
						log.debug("${cloudInfo.name}: TemplatesSync in ${new Date().time - now}ms")

						now = new Date().time
						new IpPoolsSync(context, cloudInfo).execute()
						log.debug("${cloudInfo.name}: IpPoolsSync in ${new Date().time - now}ms")

						def doInventory = cloudInfo.getConfigProperty('importExisting')
						def createNew = (doInventory == 'on' || doInventory == 'true' || doInventory == true)
						now = new Date().time
						new VirtualMachineSync(scvmmController, cloudInfo, context, this).execute(createNew)
						log.debug("${cloudInfo.name}: DatastoresSync in ${new Date().time - now}ms")
						context.async.cloud.updateCloudStatus(cloudInfo, Cloud.Status.ok, null, syncDate)
						log.debug "complete scvmm zone refresh"
						response.success = true
					} else {
						updateHypervisorStatus(scvmmController, 'error', 'unknown', 'error connecting to controller')
						context.async.cloud.updateCloudStatus(cloudInfo, Cloud.Status.error, 'error connecting', syncDate)
					}
				} else {
					updateHypervisorStatus(scvmmController, 'error', 'unknown', 'error connecting to controller')
					context.async.cloud.updateCloudStatus(cloudInfo, Cloud.Status.error, 'error connecting', syncDate)
				}
			} else {
				context.async.cloud.updateCloudStatus(cloudInfo, Cloud.Status.error, 'controller not found', syncDate)
			}
		} catch (e) {
			log.error("refresh zone error:${e}", e)
		}
		return response
	}

	def checkCommunication(cloud, node) {
		log.debug("checkCommunication: {} {}", cloud, node)
		def rtn = [success: false]
		try {
			def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
			def listResults = apiService.listAllNetworks(scvmmOpts)
			if (listResults.success == true && listResults.networks) {
				rtn.success = true
			}
		} catch (e) {
			log.error("checkCommunication error:${e}", e)
		}
		return rtn
	}

	def getScvmmController(Cloud cloud) {
		def sharedControllerId = cloud.getConfigProperty(SHARED_CONTROLLER_ERROR_KEY)
		def sharedController = sharedControllerId ? context.services.computeServer.get(sharedControllerId.toLong()) : null
		if (sharedController) {
			return sharedController
		}
		def rtn = context.services.computeServer.find(new DataQuery()
				.withFilter(ZONE_ID_FIELD, cloud.id)
				.withFilter(COMPUTE_SERVER_TYPE_CODE_FIELD, SCVMM_CONTROLLER_CODE)
				.withJoin('computeServerType'))
		if (rtn == null) {
			//old zone with wrong type
			rtn = context.services.computeServer.find(new DataQuery()
					.withFilter(ZONE_ID_FIELD, cloud.id)
					.withFilter(COMPUTE_SERVER_TYPE_CODE_FIELD, SCVMM_CONTROLLER_CODE)
					.withJoin('computeServerType'))
			if (rtn == null) {
				rtn = context.services.computeServer.find(new DataQuery()
						.withFilter(ZONE_ID_FIELD, cloud.id)
						.withFilter(SERVER_TYPE_HYPERVISOR_FIELD, SERVER_TYPE_HYPERVISOR_VALUE))
			}
			//if we have tye type
			if (rtn) {
				def serverType =
						context.async.cloud.findComputeServerTypeByCode(SCVMM_CONTROLLER_CODE).blockingGet()
				rtn.computeServerType = serverType
				context.async.computeServer.save(rtn).blockingGet()
			}
		}
		return rtn
	}

	/**
	 * Zones/Clouds are refreshed periodically by the Morpheus Environment. This includes things like caching of brownfield
	 * environments and resources such as Networks, Datastores, Resource Pools, etc. This represents the long term sync method that happens
	 * daily instead of every 5-10 minute cycle
	 * @param cloudInfo cloud
	 */
	@Override
	void refreshDaily(Cloud cloudInfo) {
		log.debug("refreshDaily: {}", cloudInfo)
		try {
			def scvmmController = getScvmmController(cloudInfo)
			if (scvmmController) {
				def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloudInfo, scvmmController)
				def hostOnline = ConnectionUtils.testHostConnectivity(scvmmOpts.sshHost, 5985, false, true, null)
				log.debug("hostOnline: {}", hostOnline)
				if (hostOnline) {
					def checkResults = checkCommunication(cloudInfo, scvmmController)
					if (checkResults.success == true) {
						removeOrphanedResourceLibraryItems(cloudInfo, scvmmController)
					}
				}
			}
		} catch (e) {
			log.error "Error on refreshDailyZone: ${e}", e
		}
	}

	/**
	 * Called when a Cloud From Morpheus is removed. This is a hook provided to take care of cleaning up any state.
	 * @param cloudInfo instance of the cloud object that is being removed.
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteCloud(Cloud cloudInfo) {
		return ServiceResponse.success()
	}

	/**
	 * Returns whether the cloud supports {@link CloudPool}
	 * @return Boolean
	 */
	@Override
	Boolean hasComputeZonePools() {
		return true
	}

	/**
	 * Returns whether a cloud supports {@link Network}
	 * @return Boolean
	 */
	@Override
	Boolean hasNetworks() {
		return true
	}

	/**
	 * Returns whether a cloud supports {@link CloudFolder}
	 * @return Boolean
	 */
	@Override
	Boolean hasFolders() {
		return false
	}

	/**
	 * Returns whether a cloud supports {@link Datastore}
	 * @return Boolean
	 */
	@Override
	Boolean hasDatastores() {
		return true
	}

	/**
	 * Returns whether a cloud supports bare metal VMs
	 * @return Boolean
	 */
	@Override
	Boolean hasBareMetal() {
		return false
	}

	/**
	 * Indicates if the cloud supports cloud-init. Returning true will allow configuration of the Cloud
	 * to allow installing the agent remotely via SSH /WinRM or via Cloud Init
	 * @return Boolean
	 */
	@Override
	Boolean hasCloudInit() {
		return true
	}

	/**
	 * Indicates if the cloud supports the distributed worker functionality
	 * @return Boolean
	 */
	@Override
	Boolean supportsDistributedWorker() {
		return false
	}

	/**
	 * Called when a server should be started. Returning a response of success will cause corresponding updates to usage
	 * records, result in the powerState of the computeServer to be set to 'on', and related instances set to 'running'
	 * @param computeServer server to start
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse startServer(ComputeServer computeServer) {
		ScvmmProvisionProvider provisionProvider = new ScvmmProvisionProvider(plugin, context)
		return provisionProvider.startServer(computeServer)
	}

	/**
	 * Called when a server should be stopped. Returning a response of success will cause corresponding updates to usage
	 * records, result in the powerState of the computeServer to be set to 'off', and related instances set to 'stopped'
	 * @param computeServer server to stop
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse stopServer(ComputeServer computeServer) {
		ScvmmProvisionProvider provisionProvider = new ScvmmProvisionProvider(plugin, context)
		return provisionProvider.stopServer(computeServer)
	}

	protected long getMaxMemory(serverInfo) {
		def maxMemory = 0L
		if (serverInfo?.memory && serverInfo?.memory?.toString()?.trim()) {
			try {
				maxMemory = serverInfo?.memory?.toString()?.toLong()
			} catch (NumberFormatException e) {
				log.warn("Invalid memory value '${serverInfo.memory}', defaulting to 0")
			}
		}
		return maxMemory
	}

	protected long getMaxStorage(serverInfo) {
		def maxStorage = 0L
		if (serverInfo?.disks && serverInfo?.disks?.toString()?.trim()) {
			try {
				maxStorage = serverInfo?.disks?.toString()?.toLong()
			} catch (NumberFormatException e) {
				log.warn("Invalid disk value '${serverInfo.disks}', defaulting to 0")
			}
		}
		return maxStorage
	}

	/**
	 * Called when a server should be deleted from the Cloud.
	 * @param computeServer server to delete
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteServer(ComputeServer computeServer) {
		log.debug("deleteServer: ${computeServer}")
		def rtn = [success: false]
		try {
			ScvmmProvisionProvider provisionProvider = new ScvmmProvisionProvider(plugin, context)
			def scvmmOpts = provisionProvider.getAllScvmmServerOpts(computeServer)

			def stopResults = apiService.stopServer(scvmmOpts, scvmmOpts.externalId)
			if(stopResults.success == true) {
				def deleteResults = apiService.deleteServer(scvmmOpts, scvmmOpts.externalId)
				if(deleteResults.success == true) {
					rtn.success = true
				}
			}
		} catch (e) {
			log.error("deleteServer error: ${e}", e)
			rtn.msg = e.message
		}
		return ServiceResponse.create(rtn)
	}

	/**
	 * Grabs the singleton instance of the provisioning provider based on the code defined in its implementation.
	 * Typically Providers are singleton and instanced in the {@link Plugin} class
	 * @param providerCode String representation of the provider short code
	 * @return the ProvisionProvider requested
	 */
	@Override
	ProvisionProvider getProvisionProvider(String providerCode) {
		return getAvailableProvisionProviders().find { it.code == providerCode }
	}

	/**
	 * Returns the default provision code for fetching a {@link ProvisionProvider} for this cloud.
	 * This is only really necessary if the provision type code is the exact same as the cloud code.
	 * @return the provision provider code
	 */
	@Override
	String getDefaultProvisionTypeCode() {
		return ScvmmProvisionProvider.PROVIDER_CODE
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	@Override
	MorpheusContext getMorpheus() {
		return this.@context
	}

	/**
	 * Returns the instance of the Plugin class that this provider is loaded from
	 * @return Plugin class contains references to other providers
	 */
	@Override
	Plugin getPlugin() {
		return this.@plugin
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return CLOUD_PROVIDER_CODE
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'SCVMM'
	}

	def validateRequiredConfigFields(fieldArray, config) {
		def errors = [:]
		fieldArray.each { field ->
			if (config[field] != null && config[field]?.size() == 0) {
				def display = field.replaceAll(/([A-Z])/, / $1/).toLowerCase()
				errors[field] = "Enter a ${display}"
			}
		}
		return errors
	}

	def validateSharedController(Cloud cloud) {
		log.debug "validateSharedController: ${cloud}"
		def rtn = [success: true]

		def sharedControllerId = cloud.getConfigProperty(SHARED_CONTROLLER_ERROR_KEY)
		if (!sharedControllerId) {
			if (cloud.id) {
				def existingControllerInZone = context.services.computeServer.find(new DataQuery()
						.withFilter(COMPUTE_SERVER_TYPE_CODE_FIELD, SCVMM_CONTROLLER_CODE)
						.withFilter(ZONE_ID_FIELD, cloud.id.toLong()))
				if (existingControllerInZone) {
					rtn.success = true
					return rtn
				}
			}
			def existingController = context.services.computeServer.find(new DataQuery()
					.withFilter('enabled', true)
					.withFilter('account.id', cloud.account.id)
					.withFilter(COMPUTE_SERVER_TYPE_CODE_FIELD, SCVMM_CONTROLLER_CODE)
					.withFilter('externalIp', cloud.getConfigProperty(HOST_CONFIG_PROPERTY)))
			if (existingController) {
				log.debug "Found another controller: ${existingController.id} in" +
						" zone: ${existingController.cloud} that should be used"
				rtn.success = false
				rtn.msg = SHARED_CONTROLLER_REQUIRED_MSG
			}
		} else {
			// Verify that the controller selected has the same Host ip as defined in the cloud
			def sharedController = context.services.computeServer.get(sharedControllerId?.toLong())
			if (sharedController?.sshHost != cloud.getConfigProperty(HOST_CONFIG_PROPERTY) &&
					sharedController?.externalIp != cloud.getConfigProperty(HOST_CONFIG_PROPERTY) ) {
				rtn.success = false
				rtn.msg = 'The selected controller is on a different host than specified for this cloud'
			}
		}
		return rtn
	}

	def removeOrphanedResourceLibraryItems(cloud, node) {
		log.debug("removeOrphanedResourceLibraryItems: {} {}", cloud, node)
		def rtn = [success:false]
		try {
			def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
			apiService.removeOrphanedResourceLibraryItems(scvmmOpts)
		} catch(e) {
			log.error("removeOrphanedResourceLibraryItems error:${e}", e)
		}
		return rtn
	}

	protected updateHypervisorStatus(server, status, powerState, msg) {
		log.debug("server: {}, status: {}, powerState: {}, msg: {}", server, status, powerState, msg)
		if (server.status != status || server.powerState != powerState) {
			server.status = status
			server.powerState = powerState
			server.statusDate = new Date()
			server.statusMessage = msg
			context.services.computeServer.save(server)
		}
	}
}
