package com.morpheusdata.scvmm

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.OptionSourceProvider
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataAndFilter
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.MorpheusUtils
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.scvmm.logging.LogInterface
import com.morpheusdata.scvmm.logging.LogWrapper
import groovy.util.logging.Slf4j

class ScvmmOptionSourceProvider extends AbstractOptionSourceProvider {

	ScvmmPlugin plugin
	MorpheusContext morpheusContext
	private ScvmmApiService apiService
	private LogInterface log = LogWrapper.instance

	ScvmmOptionSourceProvider(ScvmmPlugin plugin, MorpheusContext context) {
		this.plugin = plugin
		this.morpheusContext = context
		this.apiService = new ScvmmApiService(context)
	}

	@Override
	MorpheusContext getMorpheus() {
		return this.morpheusContext
	}

	@Override
	Plugin getPlugin() {
		return this.plugin
	}

	@Override
	String getCode() {
		return 'scvmm-option-source'
	}

	@Override
	String getName() {
		return 'SCVMM Option Source'
	}

	@Override
	List<String> getMethodNames() {
		return new ArrayList<String>([
				'scvmmCloud', 'scvmmHostGroup', 'scvmmCluster', 'scvmmLibraryShares', 'scvmmSharedControllers', 'scvmmCapabilityProfile', 'scvmmVirtualImages'
		])
	}

	def getApiConfig(cloud) {
		def rtn = apiService.getScvmmZoneOpts(morpheusContext, cloud) + apiService.getScvmmInitializationOpts(cloud)
		return rtn
	}

	def setupCloudConfig(params) {
		params = params instanceof Object[] ? params.getAt(0) : params

		def config = [
				host: params.config?.host ?: params["config[host]"],
				username: params.config?.username ?: params["config[username]"],
				hostGroup: params.config?.hostGroup ?: params["config[hostGroup]"]
		]
		def password = params.config?.password ?: params["config[password]"]


		Cloud cloud
		// get the correct credentials
		if (params.zoneId) {
			// load the cloud
			cloud = morpheusContext.services.cloud.get(params.zoneId.toLong())
			if(params.credential?.type != "local") {
				// not local creds, load from cloud or form
				if (params.credential && params.credential?.type != "local") {
					// might have new credential, load from form data
					def credentials = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
					cloud.accountCredentialLoaded = true
					cloud.accountCredentialData = credentials?.data
				} else {
					// no form data, load credentials from cloud
					def credentials = morpheusContext.services.accountCredential.loadCredentials(cloud)
					cloud.accountCredentialData = credentials?.data
					cloud.accountCredentialLoaded = true
				}
			} else if(password != '*' * 12) {
				// new password, update it
				config.password = password
			}
		} else {
			cloud = new Cloud()
			if (params.credential && params.credential?.type != "local") {
				def credentials = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
				cloud.accountCredentialLoaded = true
				cloud.accountCredentialData = credentials?.data
				log.debug("cloud.accountCredentialData: ${cloud.accountCredentialData}")
			} else {
				// local credential, set the local cred config
				config.password = password
			}
		}

		// set the config map
		cloud.setConfigMap(config)

		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))

		cloud.apiProxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null

		return cloud
	}

	def scvmmCloud(params) {
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listClouds(apiConfig)
		}
		log.debug("listClouds: ${results}")
		def optionList = []
		if(results.clouds?.size() > 0) {
			optionList << [name: "Select a Cloud", value: ""]
			optionList += results.clouds?.collect { [name: it.Name, value: it.ID] }
		} else {
			optionList = [[name:"No Clouds Found: verify credentials above", value:""]]
		}
		return optionList
	}

	def scvmmHostGroup(params) {
		log.debug("scvmmHostGroup: ${params}")
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listHostGroups(apiConfig)
		}
		log.debug("listHostGroups: ${results}")
		def optionList = []
		if(results.hostGroups?.size() > 0) {
			optionList << [name: "Select", value: ""]
			optionList += results.hostGroups?.collect { [name: it.path, value: it.path] }
		} else {
			optionList = [[name:"No Host Groups found", value:""]]
		}
		return optionList
	}

	def scvmmCluster(params) {
		log.debug("scvmmCluster: ${params}")
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listClusters(apiConfig)
		}
		log.debug("listClusters: ${results}")
		def optionList = []
		if(results.clusters?.size() > 0) {
			optionList << [name: "All", value: ""]
			optionList += results.clusters?.collect { [name: it.name, value: it.id] }
		} else {
			optionList = [[name:"No Clusters found: check your config", value:""]]
		}
		return optionList
	}

	def scvmmLibraryShares(params) {
		log.debug("scvmmLibraryShares: ${params}")
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listLibraryShares(apiConfig)
		}
		log.debug("listLibraryShares: ${results}")
		return results.libraryShares.size() > 0 ? results.libraryShares?.collect { [name: it.Path, value: it.Path] } : [[name:"No Library Shares found", value:""]]
	}

	def scvmmSharedControllers(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug("scvmmSharedControllers: ${params}")

		def orFilters = []
		if(params.zoneId) {
			orFilters << new DataFilter('zone.id', params.zoneId)
		}
		if(params.config?.host) {
			orFilters << new DataFilter('sshHost', params.config.host)
		}

		def sharedControllers = morpheusContext.services.computeServer.find(new DataQuery().withFilters(
				new DataFilter('enabled', true),
				new DataFilter('computeServerType.code', 'scvmmController'),
				new DataOrFilter(orFilters)
		))

		return sharedControllers?.collect{[name: it.name, value: it.id]}
	}

	def scvmmCapabilityProfile(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		def tmpZone = params.zoneId ? morpheusContext.services.cloud.get(params.zoneId?.toLong()) : null

		def capabilityProfiles = tmpZone?.getConfigProperty('capabilityProfiles')

		def profiles = []
		if(capabilityProfiles) {
			capabilityProfiles.each{ it ->
				profiles << [name: it, value: it]
			}
		} else {
			profiles << [name:'Not required', value: -1]
		}
		return profiles
	}

	/*def scvmmHost(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug("scvmmHost: ${params}")

		def tmpZone = params.zoneId ? morpheusContext.services.cloud.get(params.zoneId?.toLong()) : null

		if(tmpZone?.getConfigProperty('hideHostSelection') == 'on') {
			return [[name:'Auto', value: '']]
		}

		def resourcePoolId = params.resourcePoolId?.toString()?.isNumber() ? params.resourcePoolId.toLong() :
				(params.config?.resourcePool != 'null' ?
						(params.config?.resourcePool?.toString()?.isNumber() ?
								params.config.resourcePool?.toLong() : null) : null)

		def query = new DataQuery()

		// Join account
		query.withJoin('account')
				.withFilter('account.id', tmpZone.account.id)
				.withFilter('enabled', true)
				.withFilter('powerState', 'on')
				.withFilter('category', "scvmm.host.${tmpZone.id}")

		if(resourcePoolId) {
			query.withJoin('resourcePool')
					.withFilter('resourcePool.id', resourcePoolId)
		}

		def results = morpheusContext.services.computeServer.listIdentityProjections(query.withSort('name', DataQuery.SortOrder.asc))

		return results.collect { host -> [name: host.name, value: host.id] }
	}*/

	def scvmmVirtualImages(Object params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug("scvmmVirtualImages: ${params}")

		def account = params.currentUser?.account
		def tmpZone = params.zoneId ? morpheusContext.services.cloud.get(params.zoneId?.toLong()) : null
		def regionCode = tmpZone?.regionCode

		// Construct the query with all the necessary filters
		def query = new DataQuery()

		// Left joins
		query.withJoin("accounts")
				.withJoin("owner")
				.withJoin("locations")

		// OR filters for visibility/ownership
		def orFilters = []
		orFilters << new DataFilter("visibility", "public")
		orFilters << new DataFilter("accounts.id", params.accountId?.toLong())
		orFilters << new DataFilter("owner.id", params.accountId?.toLong())
		orFilters << new DataFilter("owner.id", null)
		query.withFilter(new DataOrFilter(orFilters))

		// Basic filters
		query.withFilter("deleted", false)
		query.withFilter("imageType", 'in', ['vhd', 'vhdx', 'vmdk'])

		// Zone and region filters
		def zoneRegionOrFilters = []

		if (tmpZone) {
			// Category filter
			zoneRegionOrFilters << new DataFilter("category", "scvmm.image.${tmpZone?.id}")

			// Reference filters
			def refAndFilters = []
			refAndFilters << new DataFilter("refType", "ComputeZone")
			refAndFilters << new DataFilter("refId", tmpZone.id.toString())
			zoneRegionOrFilters << new DataAndFilter(refAndFilters)

			// Location filters
			def locAndFilters = []
			locAndFilters << new DataFilter("locations.refType", "ComputeZone")
			locAndFilters << new DataFilter("locations.refId", tmpZone.id)
			zoneRegionOrFilters << new DataAndFilter(locAndFilters)
		}

		if (regionCode) {
			zoneRegionOrFilters << new DataFilter("imageRegion", regionCode)
			zoneRegionOrFilters << new DataFilter("locations.imageRegion", regionCode)
			zoneRegionOrFilters << new DataFilter("userUploaded", true)
		}

		query.withFilter(new DataOrFilter(zoneRegionOrFilters))

		try {
			def results = morpheusContext.services.virtualImage.listIdentityProjections(query.withSort("name", DataQuery.SortOrder.asc))
			return results.collect { vimage -> [name: vimage.name, value: vimage.id] }
		} catch (e) {
			log.error("scvmmVirtualImages error: ${e}", e)
			return []
		}
	}

}
