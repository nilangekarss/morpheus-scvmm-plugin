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
		return new Icon(path:'scvmm-circular.svg', darkPath:'scvmm-circular.svg')
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
				name: 'SCVMM Host',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.host',
				fieldName: 'host',
				displayOrder: displayOrder,
				fieldCode: 'gomorpheus.scvmm.option.host',
				fieldLabel:'SCVMM Host',
				required: true,
				inputType: OptionType.InputType.TEXT,
				fieldContext:'config',
		)
		options << new OptionType(
				name: 'Credentials',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.credential',
				fieldName: 'type',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.label.credentials',
				fieldLabel:'Credentials',
				required: true,
				defaultValue:'local',
				inputType: OptionType.InputType.CREDENTIAL,
				fieldContext: 'credential',
				optionSource:'credentials',
				config: '{"credentialTypes":["username-password"]}'
		)
		options << new OptionType(
				name: 'Username',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.username',
				fieldName: 'username',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.Username',
				fieldLabel:'Username',
				required: true,
				inputType: OptionType.InputType.TEXT,
				fieldContext: 'config',
				localCredential: true
		)
		options << new OptionType(
				name: 'Password',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.password',
				fieldName: 'password',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.Password',
				fieldLabel:'Password',
				required: true,
				inputType: OptionType.InputType.PASSWORD,
				fieldContext: 'config',
				localCredential: true
		)
		options << new OptionType(
				name: 'Cloud',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.cloud',
				fieldName: 'regionCode',
				optionSourceType:'scvmm',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.Cloud',
				fieldLabel:'Cloud',
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmCloud',
				fieldContext:'domain',
				noBlank: true,
				dependsOn: 'config.host, config.username, config.password, credential.type, credential.username, credential.password'
		)
		options << new OptionType(
				name: 'Host Group',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.hostGroup',
				fieldName: 'hostGroup',
				optionSourceType:'scvmm',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.HostGroup',
				fieldLabel:'Host Group',
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmHostGroup',
				fieldContext:'config',
				noBlank: true,
				dependsOn: 'config.host, config.username, config.password, credential.type, credential.username, credential.password',
		)
		options << new OptionType(
				name: 'Cluster',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.Cluster',
				fieldName: 'cluster',
				optionSourceType:'scvmm',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.Cluster',
				fieldLabel:'Cluster',
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmCluster',
				fieldContext:'config',
				noBlank: true,
				dependsOn: 'config.host, config.username, config.password, config.hostGroup, credential.type, credential.username, credential.password'
		)
		options << new OptionType(
				name: 'Library Share',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.libraryShare',
				fieldName: 'libraryShare',
				optionSourceType:'scvmm',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.LibraryShare',
				fieldLabel:'Library Share',
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmLibraryShares',
				fieldContext:'config',
				noBlank: true,
				dependsOn: 'config.host, config.username, config.password, credential.type, credential.username, credential.password'
		)
		options << new OptionType(
				name: 'Shared Controller',
				category:'zoneType.scvmm',
				code: 'zoneType.scvmm.sharedController',
				fieldName: 'sharedController',
				optionSourceType:'scvmm',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.SharedController',
				fieldLabel:'Shared Controller',
				required: false,
				inputType: OptionType.InputType.SELECT,
				optionSource: 'scvmmSharedControllers',
				fieldContext:'config',
				editable: false
		)
		options << new OptionType(
				name: 'Working Path',
				code: 'zoneType.scvmm.workingPath',
				fieldName: 'workingPath',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.WorkingPath',
				fieldLabel:'Working Path',
				required: true,
				inputType: OptionType.InputType.TEXT,
				defaultValue: 'c:\\Temp'
		)
		options << new OptionType(
				name: 'Disk Path',
				code: 'zoneType.scvmm.diskPath',
				fieldName: 'diskPath',
				displayOrder: displayOrder += 10,
				fieldCode: 'gomorpheus.optiontype.DiskPath',
				fieldLabel:'Disk Path',
				required: true,
				inputType: OptionType.InputType.TEXT,
				defaultValue:'c:\\VirtualDisks'
		)
		options << new OptionType(
				name: 'Hide Host Selection From Users',
				code: 'zoneType.scvmm.hideHostSelection',
				fieldName: 'HideHostSelectionFromUsers',
				displayOrder: displayOrder += 10,
				fieldLabel: 'Hide Host Selection From Users',
				required: false,
				inputType: OptionType.InputType.CHECKBOX,
				fieldContext: 'config',
		)
		options << new OptionType(
				name: 'Inventory Existing Instances',
				code: 'zoneType.scvmm.importExisting',
				fieldName: 'importExisting',
				displayOrder: displayOrder += 10,
				fieldLabel: 'Inventory Existing Instances',
				required: false,
				inputType: OptionType.InputType.CHECKBOX,
				fieldContext: 'config',
		)
		options << new OptionType(
				name: 'Enable Hypervisor Console',
				code: 'zoneType.scvmm.enableHypervisorConsole',
				fieldName: 'enableHypervisorConsole',
				displayOrder: displayOrder += 10,
				fieldLabel: 'Enable Hypervisor Console',
				required: false,
				inputType: OptionType.InputType.CHECKBOX,
				fieldContext: 'config',
		)
		options << new OptionType(
				name: 'Install Agent',
				code: 'gomorpheus.label.installAgent',
				inputType: OptionType.InputType.CHECKBOX,
				fieldName: 'installAgent',
				fieldContext: 'config',
				fieldCode: 'gomorpheus.label.installAgent',
				fieldLabel: 'Install Agent',
				fieldGroup: 'Advanced',
				displayOrder: 999,
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
	 * Grabs all {@link ComputeServerType} objects that this CloudProvider can represent during a sync or during a provision.
	 * @return collection of ComputeServerType
	 */
	@Override
	Collection<ComputeServerType> getComputeServerTypes() {
		Collection<ComputeServerType> serverTypes = []

		// Host option type is used by multiple compute server types.
		OptionType hostOptionType = new OptionType(code:'computeServerType.scvmm.capabilityProfile', inputType: OptionType.InputType.SELECT,
				name:'capability profile', category:'provisionType.scvmm', optionSourceType:'scvmm', fieldName:'scvmmCapabilityProfile',
				fieldCode: 'gomorpheus.optiontype.CapabilityProfile', fieldLabel:'Capability Profile', fieldContext:'config', fieldGroup:'Options',
				required:true, enabled:true, optionSource:'scvmmCapabilityProfile', editable:true, global:false, placeHolder:null, helpBlock:'',
				defaultValue:null, custom:false, displayOrder:10, fieldClass:null
		)

		//scvmm
		serverTypes << new ComputeServerType(code:'scvmmController', name:'SCVMM Manager', description:'', platform:PlatformType.windows,
				nodeType:'morpheus-scvmm-node', enabled:true, selectable:false, externalDelete:false, managed:false, controlPower:false,
				controlSuspend:false, creatable:false, computeService:'scvmmComputeService', displayOrder:0, hasAutomation:false, containerHypervisor:false,
				bareMetalHost:false, vmHypervisor:true, agentType: ComputeServerType.AgentType.node, provisionTypeCode:'scvmm-hypervisor'
		)

		//vms
		serverTypes << new ComputeServerType(code:'scvmmHypervisor', name:'SCVMM Hypervisor', description:'', platform:PlatformType.windows,
				nodeType:'morpheus-scvmm-node', enabled:true, selectable:false, externalDelete:false, managed:false, controlPower:false,
				controlSuspend:false, creatable:false, computeService:'scvmmComputeService', displayOrder:0, hasAutomation:false, containerHypervisor:false,
				bareMetalHost:false, vmHypervisor:true, agentType: ComputeServerType.AgentType.node, provisionTypeCode:'scvmm-hypervisor'
		)
		serverTypes << new ComputeServerType(code:'scvmmWindows', name:'SCVMM Windows Node', description:'', platform:PlatformType.windows,
				nodeType:'morpheus-windows-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true,
				controlSuspend:false, creatable:false, computeService:'scvmmComputeService', displayOrder:7, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:false, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.node, guestVm:true,
				provisionTypeCode:'scvmm'
		)
		serverTypes << new ComputeServerType(code:'scvmmVm', name:'SCVMM Instance', description:'', platform:PlatformType.linux,
				nodeType:'morpheus-vm-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true, controlSuspend:false,
				creatable:false, computeService:'scvmmComputeService', displayOrder: 0, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:false, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.guest, guestVm:true,
				provisionTypeCode:'scvmm'
		)
		//windows container host - not used
		serverTypes << new ComputeServerType(code:'scvmmWindowsVm', name:'SCVMM Windows Instance', description:'', platform:PlatformType.windows,
				nodeType:'morpheus-windows-vm-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true,
				controlSuspend:false, creatable:false, computeService:'scvmmComputeService', displayOrder: 0, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:false, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.guest, guestVm:true,
				provisionTypeCode:'scvmm'
		)
		serverTypes << new ComputeServerType(code:'scvmmUnmanaged', name:'SCVMM Instance', description:'scvmm vm', platform:PlatformType.linux,
				nodeType:'unmanaged', enabled:true, selectable:false, externalDelete:true, managed:false, controlPower:true, controlSuspend:false,
				creatable:false, computeService:'scvmmComputeService', displayOrder:99, hasAutomation:false, containerHypervisor:false,
				bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.guest, managedServerType:'scvmmVm', guestVm:true,
				provisionTypeCode:'scvmm'
		)

		//docker
		serverTypes << new ComputeServerType(code:'scvmmLinux', name:'SCVMM Docker Host', description:'', platform:PlatformType.linux,
				nodeType:'morpheus-node', enabled:true, selectable:false, externalDelete:true, managed:true, controlPower:true, controlSuspend:false,
				creatable:false, computeService:'scvmmComputeService', displayOrder: 6, hasAutomation:true, reconfigureSupported:true,
				containerHypervisor:true, bareMetalHost:false, vmHypervisor:false, agentType:ComputeServerType.AgentType.node, containerEngine:'docker',
				provisionTypeCode:'scvmm', computeTypeCode:'docker-host', optionTypes:[hostOptionType]
		)

		//kubernetes
		serverTypes << new ComputeServerType(code:'scvmmKubeMaster', name:'SCVMM Kubernetes Master', description:'', platform:PlatformType.linux,
				nodeType:'kube-master', hasMaintenanceMode: true, reconfigureSupported: true, enabled:true, selectable:false, externalDelete:true, managed:true,
				controlPower:true, controlSuspend:true, creatable:true, supportsConsoleKeymap: true, computeService:'scvmmComputeService',
				displayOrder:10, hasAutomation:true, containerHypervisor:true, bareMetalHost:false, vmHypervisor:false,
				agentType:ComputeServerType.AgentType.host, containerEngine:'docker', provisionTypeCode:'scvmm', computeTypeCode:'kube-master',
				optionTypes:[hostOptionType]
		)
		serverTypes << new ComputeServerType(code:'scvmmKubeWorker', name:'SCVMM Kubernetes Worker', description:'', platform:PlatformType.linux,
				nodeType:'kube-worker', hasMaintenanceMode: true, reconfigureSupported: true, enabled:true, selectable:false, externalDelete:true, managed:true,
				controlPower:true, controlSuspend:true, creatable:true, supportsConsoleKeymap: true, computeService:'scvmmComputeService',
				displayOrder:10, hasAutomation:true, containerHypervisor:true, bareMetalHost:false, vmHypervisor:false,
				agentType:ComputeServerType.AgentType.guest, containerEngine:'docker', provisionTypeCode:'scvmm', computeTypeCode:'kube-worker',
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
				def requiredFields = ['host', 'workingPath', 'diskPath', 'libraryShare']
				def scvmmOpts = apiService.getScvmmZoneOpts(context, cloudInfo)
				def zoneConfig = cloudInfo.getConfigMap()
				rtn.errors = validateRequiredConfigFields(requiredFields, zoneConfig)
				// Verify that a shared controller is selected if we already have an scvmm zone pointed to this host (MUST SHARE THE CONTROLLER)
				def validateControllerResults = validateSharedController(cloudInfo)
				if (!validateControllerResults.success) {
					rtn.errors['sharedController'] = validateControllerResults.msg ?: 'You must specify a shared controller'
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
		initializePoolServer(cloudInfo)

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
		def sharedController = cloud.getConfigProperty('sharedController')
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
						new DataFilter('zone.id', cloud.id),
						new DataOrFilter(
								new DataFilter('hostname', serverInfo.hostname),
								new DataFilter('name', serverInfo.hostname)
						)
				))
				if(!newServer) {
					newServer = new ComputeServer()
					newServer.account = cloud.account
					newServer.cloud = cloud
					newServer.computeServerType = context.async.cloud.findComputeServerTypeByCode("scvmmController").blockingGet()
					// Create proper OS code format
					newServer.serverOs = new OsType(code: versionCode)
					newServer.name = serverInfo.hostname
					newServer = context.services.computeServer.create(newServer)
				}
				if(serverInfo.hostname) {
					newServer.hostname = serverInfo.hostname
				}
				newServer.sshHost = cloud.getConfigProperty('host')
				newServer.internalIp = newServer.sshHost
				newServer.externalIp = newServer.sshHost
				newServer.sshUsername = apiService.getUsername(cloud)
				newServer.sshPassword = apiService.getPassword(cloud)
				newServer.setConfigProperty('workingPath', cloud.getConfigProperty('workingPath'))
				newServer.setConfigProperty('diskPath', cloud.getConfigProperty('diskPath'))
			}

			def maxStorage = serverInfo?.disks? serverInfo.disks.toLong()  :0L
			def maxMemory = serverInfo?.memory? serverInfo.memory.toLong() : 0L
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
			newServer.serverType = 'hypervisor'
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
			def scvmmController = apiService.getScvmmController(cloudInfo)
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
						new IpPoolsSync(context, cloudInfo, lookUpPoolServer(cloudInfo)).execute()
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

	/**
	 * Zones/Clouds are refreshed periodically by the Morpheus Environment. This includes things like caching of brownfield
	 * environments and resources such as Networks, Datastores, Resource Pools, etc. This represents the long term sync method that happens
	 * daily instead of every 5-10 minute cycle
	 * @param cloudInfo cloud
	 */
	@Override
	void refreshDaily(Cloud cloudInfo) {
		log.debug("refreshDaily: {}", cloudInfo)
		initializePoolServer(cloudInfo)
		try {
			def scvmmController = apiService.getScvmmController(cloudInfo)
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
		def poolServer = lookUpPoolServer(cloudInfo)
		if (poolServer) {
			def pools = context.services.network.pool.list(new DataQuery().withFilters(
					new DataFilter('refType', 'ComputeZone'),
					new DataFilter('refId', cloudInfo.id)
			))

			if (pools) {
				for (def pool in pools) {
					pool.poolServer = null
				}
				// Delete NetworkPools, so we can clear references to pool server before deleting
				context.async.network.pool.remove(poolServer.id, pools).blockingGet()
			}
			context.async.network.poolServer.bulkRemove([poolServer]).blockingGet()
		}
		return ServiceResponse.success()
	}

	/**
	 * Returns whether the cloud supports {@link CloudPool}
	 * @return Boolean
	 */
	@Override
	Boolean hasComputeZonePools() {
		return false
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

		def sharedControllerId = cloud.getConfigProperty('sharedController')
		if (!sharedControllerId) {
			if (cloud.id) {
				def existingControllerInZone = context.services.computeServer.find(new DataQuery()
						.withFilter('computeServerType.code', 'scvmmController')
						.withFilter('zone.id', cloud.id.toLong()))
				if (existingControllerInZone) {
					rtn.success = true
					return rtn
				}
			}
			def existingController = context.services.computeServer.find(new DataQuery()
					.withFilter('enabled', true)
					.withFilter('account.id', cloud.account.id)
					.withFilter('computeServerType.code', 'scvmmController')
					.withFilter('externalIp', cloud.getConfigProperty('host')))
			if (existingController) {
				log.debug "Found another controller: ${existingController.id} in zone: ${existingController.cloud} that should be used"
				rtn.success = false
				rtn.msg = 'You must specify a shared controller'
			}
		} else {
			// Verify that the controller selected has the same Host ip as defined in the cloud
			def sharedController = context.services.computeServer.get(sharedControllerId?.toLong())
			if (sharedController?.sshHost != cloud.getConfigProperty('host') && sharedController?.externalIp != cloud.getConfigProperty('host') ) {
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

	private updateHypervisorStatus(server, status, powerState, msg) {
		log.debug("server: {}, status: {}, powerState: {}, msg: {}", server, status, powerState, msg)
		if (server.status != status || server.powerState != powerState) {
			server.status = status
			server.powerState = powerState
			server.statusDate = new Date()
			server.statusMessage = msg
			context.services.computeServer.save(server)
		}
	}

	private void initializePoolServer(Cloud cloudInfo) {
		def poolServer = lookUpPoolServer(cloudInfo)
		if (!poolServer) {
			log.info("Creating default pool server for NPE cloud ${cloudInfo.id}")

			context.services.network.poolServer.create(new NetworkPoolServer(
					name: "SCVMM Network Pool Server - ${cloudInfo.id}",
					internalId: getNetworkPoolServerId(cloudInfo),
					type: new NetworkPoolServerType(code: ScvmmNetworkPoolProvider.NETWORK_POOL_PROVIDER_CODE),
					account: cloudInfo.account,
					visible: false,
			))
		}
	}

	private NetworkPoolServer lookUpPoolServer(Cloud cloudInfo) {
		context.services.network.poolServer.find(
				new DataQuery().withFilters(
						new DataFilter("internalId", getNetworkPoolServerId(cloudInfo)),
						new DataFilter("account.id", cloudInfo.account.id)
				)
		)
	}

	private static def getNetworkPoolServerId(Cloud cloudInfo) {
		"${ScvmmNetworkPoolProvider.NETWORK_POOL_PROVIDER_CODE}.${cloudInfo.id}"
	}
}
