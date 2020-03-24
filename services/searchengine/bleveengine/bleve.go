package bleveengine

import (
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mattermost/mattermost-server/v5/jobs"
	"github.com/mattermost/mattermost-server/v5/mlog"
	"github.com/mattermost/mattermost-server/v5/model"

	"github.com/blugelabs/bleve"
	"github.com/blugelabs/bleve/analysis/analyzer/keyword"
	"github.com/blugelabs/bleve/analysis/analyzer/standard"
	"github.com/blugelabs/bleve/mapping"
	"github.com/blugelabs/bleve/search/query"
)

type BleveEngine struct {
	postIndex    bleve.Index
	userIndex    bleve.Index
	channelIndex bleve.Index
	cfg          *model.Config
	jobServer    *jobs.JobServer
	indexSync    bool
}

var emailRegex = regexp.MustCompile(`^[^\s"]+@[^\s"]+$`)

var keywordMapping *mapping.FieldMapping
var standardMapping *mapping.FieldMapping
var dateMapping *mapping.FieldMapping

func init() {
	keywordMapping = bleve.NewTextFieldMapping()
	keywordMapping.Analyzer = keyword.Name

	standardMapping = bleve.NewTextFieldMapping()
	standardMapping.Analyzer = standard.Name

	dateMapping = bleve.NewNumericFieldMapping()
}

func getChannelIndexMapping() *mapping.IndexMappingImpl {
	channelMapping := bleve.NewDocumentMapping()
	channelMapping.AddFieldMappingsAt("Id", keywordMapping)
	channelMapping.AddFieldMappingsAt("TeamId", keywordMapping)
	channelMapping.AddFieldMappingsAt("NameSuggest", keywordMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping("_default", channelMapping)

	return indexMapping
}

func getPostIndexMapping() *mapping.IndexMappingImpl {
	postMapping := bleve.NewDocumentMapping()
	postMapping.AddFieldMappingsAt("Id", keywordMapping)
	postMapping.AddFieldMappingsAt("TeamId", keywordMapping)
	postMapping.AddFieldMappingsAt("ChannelId", keywordMapping)
	postMapping.AddFieldMappingsAt("UserId", keywordMapping)
	postMapping.AddFieldMappingsAt("CreateAt", dateMapping)
	postMapping.AddFieldMappingsAt("Message", standardMapping)
	postMapping.AddFieldMappingsAt("Type", keywordMapping)
	postMapping.AddFieldMappingsAt("Hashtags", standardMapping)
	postMapping.AddFieldMappingsAt("Attachments", standardMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping("_default", postMapping)

	return indexMapping
}

func getUserIndexMapping() *mapping.IndexMappingImpl {
	userMapping := bleve.NewDocumentMapping()
	userMapping.AddFieldMappingsAt("Id", keywordMapping)
	userMapping.AddFieldMappingsAt("SuggestionsWithFullname", keywordMapping)
	userMapping.AddFieldMappingsAt("SuggestionsWithoutFullname", keywordMapping)
	userMapping.AddFieldMappingsAt("TeamsIds", keywordMapping)
	userMapping.AddFieldMappingsAt("ChannelsIds", keywordMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping("_default", userMapping)

	return indexMapping
}

func createOrOpenIndex(cfg *model.Config, indexName string, mapping *mapping.IndexMappingImpl) (bleve.Index, error) {
	indexPath := filepath.Join(*cfg.BleveSettings.IndexDir, indexName+".bleve")
	if index, err := bleve.Open(indexPath); err == nil {
		return index, nil
	}

	index, err := bleve.New(indexPath, mapping)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func NewBleveEngine(cfg *model.Config, jobServer *jobs.JobServer) *BleveEngine {
	return &BleveEngine{
		cfg:       cfg,
		jobServer: jobServer,
	}
}

func (b *BleveEngine) Start() *model.AppError {
	if !*b.cfg.BleveSettings.EnableIndexing || *b.cfg.BleveSettings.IndexDir == "" {
		return nil
	}

	mlog.Warn("Starting Bleve")
	var err error
	b.postIndex, err = createOrOpenIndex(b.cfg, "posts", getPostIndexMapping())
	if err != nil {
		return model.NewAppError("Bleveengine.Start", "bleveengine.create_post_index.error", nil, err.Error(), http.StatusInternalServerError)
	}

	b.userIndex, err = createOrOpenIndex(b.cfg, "users", getUserIndexMapping())
	if err != nil {
		return model.NewAppError("Bleveengine.Start", "bleveengine.create_user_index.error", nil, err.Error(), http.StatusInternalServerError)
	}

	b.channelIndex, err = createOrOpenIndex(b.cfg, "channels", getChannelIndexMapping())
	if err != nil {
		return model.NewAppError("Bleveengine.Start", "bleveengine.create_channel_index.error", nil, err.Error(), http.StatusInternalServerError)
	}

	return nil
}

func (b *BleveEngine) Stop() *model.AppError {
	mlog.Warn("Stopping Bleve")
	if b.IsActive() {
		if err := b.postIndex.Close(); err != nil {
			return model.NewAppError("Bleveengine.Stop", "bleveengine.stop_post_index.error", nil, err.Error(), http.StatusInternalServerError)
		}

		if err := b.userIndex.Close(); err != nil {
			return model.NewAppError("Bleveengine.Stop", "bleveengine.stop_user_index.error", nil, err.Error(), http.StatusInternalServerError)
		}

		if err := b.channelIndex.Close(); err != nil {
			return model.NewAppError("Bleveengine.Stop", "bleveengine.stop_channel_index.error", nil, err.Error(), http.StatusInternalServerError)
		}
	}
	return nil
}

func (b *BleveEngine) IsActive() bool {
	return *b.cfg.BleveSettings.EnableIndexing && *b.cfg.BleveSettings.IndexDir != ""
}

func (b *BleveEngine) IsIndexingSync() bool {
	return b.indexSync
}

func (b *BleveEngine) RefreshIndexes() *model.AppError {
	return nil
}

func (b *BleveEngine) GetVersion() int {
	return 0
}

func (b *BleveEngine) GetName() string {
	return "bleve"
}

func (b *BleveEngine) IndexPost(post *model.Post, teamId string) *model.AppError {
	blvPost := BLVPostFromPost(post, teamId)
	if err := b.postIndex.Index(blvPost.Id, blvPost); err != nil {
		return model.NewAppError("Bleveengine.IndexPost", "bleveengine.index_post.error", nil, err.Error(), http.StatusInternalServerError)
	}
	return nil
}

func (b *BleveEngine) SearchPosts(channels *model.ChannelList, searchParams []*model.SearchParams, page, perPage int) ([]string, model.PostSearchMatches, *model.AppError) {
	channelQueries := []query.Query{}
	for _, channel := range *channels {
		channelIdQ := bleve.NewTermQuery(channel.Id)
		channelIdQ.SetField("ChannelId")
		channelQueries = append(channelQueries, channelIdQ)
	}
	channelDisjunctionQ := bleve.NewDisjunctionQuery(channelQueries...)

	termQueries := []query.Query{}
	filters := []query.Query{}
	notFilters := []query.Query{}
	for i, params := range searchParams {
		// Date, channels and FromUsers filters come in all
		// searchParams iteration, and as they are global to the
		// query, we only need to process them once
		if i == 0 {
			if len(params.InChannels) > 0 {
				inChannels := []query.Query{}
				for _, channelId := range params.InChannels {
					channelQ := bleve.NewTermQuery(channelId)
					channelQ.SetField("ChannelId")
					inChannels = append(inChannels, channelQ)
				}
				filters = append(filters, bleve.NewDisjunctionQuery(inChannels...))
			}

			if len(params.ExcludedChannels) > 0 {
				excludedChannels := []query.Query{}
				for _, channelId := range params.ExcludedChannels {
					channelQ := bleve.NewTermQuery(channelId)
					channelQ.SetField("ChannelId")
					excludedChannels = append(excludedChannels, channelQ)
				}
				notFilters = append(notFilters, bleve.NewDisjunctionQuery(excludedChannels...))
			}

			if len(params.FromUsers) > 0 {
				fromUsers := []query.Query{}
				for _, userId := range params.FromUsers {
					userQ := bleve.NewTermQuery(userId)
					userQ.SetField("UserId")
					fromUsers = append(fromUsers, userQ)
				}
				filters = append(filters, bleve.NewDisjunctionQuery(fromUsers...))
			}

			if len(params.ExcludedUsers) > 0 {
				excludedUsers := []query.Query{}
				for _, userId := range params.ExcludedUsers {
					userQ := bleve.NewTermQuery(userId)
					userQ.SetField("UserId")
					excludedUsers = append(excludedUsers, userQ)
				}
				notFilters = append(notFilters, bleve.NewDisjunctionQuery(excludedUsers...))
			}

			if params.OnDate != "" {
				before, after := params.GetOnDateMillis()
				beforeFloat64 := float64(before)
				afterFloat64 := float64(after)
				onDateQ := bleve.NewNumericRangeQuery(&beforeFloat64, &afterFloat64)
				onDateQ.SetField("CreateAt")
				filters = append(filters, onDateQ)
			} else {
				if params.AfterDate != "" || params.BeforeDate != "" {
					var min, max *float64
					if params.AfterDate != "" {
						minf := float64(params.GetAfterDateMillis())
						min = &minf
					}

					if params.BeforeDate != "" {
						maxf := float64(params.GetBeforeDateMillis())
						max = &maxf
					}

					dateQ := bleve.NewNumericRangeQuery(min, max)
					dateQ.SetField("CreateAt")
					filters = append(filters, dateQ)
				}

				if params.ExcludedAfterDate != "" {
					minf := float64(params.GetExcludedAfterDateMillis())
					dateQ := bleve.NewNumericRangeQuery(&minf, nil)
					dateQ.SetField("CreateAt")
					notFilters = append(notFilters, dateQ)
				}

				if params.ExcludedBeforeDate != "" {
					maxf := float64(params.GetExcludedBeforeDateMillis())
					dateQ := bleve.NewNumericRangeQuery(nil, &maxf)
					dateQ.SetField("CreateAt")
					notFilters = append(notFilters, dateQ)
				}

				if params.ExcludedDate != "" {
					before, after := params.GetExcludedDateMillis()
					beforef := float64(before)
					afterf := float64(after)
					onDateQ := bleve.NewNumericRangeQuery(&beforef, &afterf)
					onDateQ.SetField("CreateAt")
					notFilters = append(notFilters, onDateQ)
				}
			}
		}

		messageQ := bleve.NewMatchQuery(params.Terms)
		messageQ.SetField("Message")
		termQueries = append(termQueries, messageQ)
	}

	var allTermsQ query.Query
	if searchParams[0].OrTerms {
		allTermsQ = bleve.NewDisjunctionQuery(termQueries...)
	} else {
		allTermsQ = bleve.NewConjunctionQuery(termQueries...)
	}

	query := bleve.NewBooleanQuery()
	query.AddMust(
		channelDisjunctionQ,
		allTermsQ,
	)
	if len(filters) > 0 {
		query.AddMust(bleve.NewConjunctionQuery(filters...))
	}
	if len(notFilters) > 0 {
		query.AddMustNot(notFilters...)
	}

	search := bleve.NewSearchRequest(query)
	results, err := b.postIndex.Search(search)
	if err != nil {
		return nil, nil, model.NewAppError("Bleveengine.SearchPosts", "bleveengine.search_posts.error", nil, err.Error(), http.StatusInternalServerError)
	}

	postIds := []string{}
	matches := model.PostSearchMatches{}

	for _, r := range results.Hits {
		postIds = append(postIds, r.ID)
	}

	return postIds, matches, nil
}

func (b *BleveEngine) DeletePost(post *model.Post) *model.AppError {
	if err := b.postIndex.Delete(post.Id); err != nil {
		return model.NewAppError("Bleveengine.DeletePost", "bleveengine.delete_post.error", nil, err.Error(), http.StatusInternalServerError)
	}
	return nil
}

func (b *BleveEngine) IndexChannel(channel *model.Channel) *model.AppError {
	blvChannel := BLVChannelFromChannel(channel)
	if err := b.channelIndex.Index(blvChannel.Id, blvChannel); err != nil {
		return model.NewAppError("Bleveengine.IndexChannel", "bleveengine.index_channel.error", nil, err.Error(), http.StatusInternalServerError)
	}
	return nil
}

func (b *BleveEngine) SearchChannels(teamId, term string) ([]string, *model.AppError) {
	teamIdQ := bleve.NewTermQuery(teamId)
	teamIdQ.SetField("TeamId")
	queries := []query.Query{teamIdQ}

	if term != "" {
		nameSuggestQ := bleve.NewPrefixQuery(strings.ToLower(term))
		nameSuggestQ.SetField("NameSuggest")
		queries = append(queries, nameSuggestQ)
	}

	query := bleve.NewSearchRequest(bleve.NewConjunctionQuery(queries...))
	results, err := b.channelIndex.Search(query)
	if err != nil {
		return nil, model.NewAppError("Bleveengine.SearchChannels", "bleveengine.search_channels.error", nil, err.Error(), http.StatusInternalServerError)
	}

	channelIds := []string{}
	for _, result := range results.Hits {
		channelIds = append(channelIds, result.ID)
	}

	return channelIds, nil
}

func (b *BleveEngine) DeleteChannel(channel *model.Channel) *model.AppError {
	if err := b.channelIndex.Delete(channel.Id); err != nil {
		return model.NewAppError("Bleveengine.DeleteChannel", "bleveengine.delete_channel.error", nil, err.Error(), http.StatusInternalServerError)
	}
	return nil
}

func (b *BleveEngine) IndexUser(user *model.User, teamsIds, channelsIds []string) *model.AppError {
	blvUser := BLVUserFromUserAndTeams(user, teamsIds, channelsIds)
	if err := b.userIndex.Index(blvUser.Id, blvUser); err != nil {
		return model.NewAppError("Bleveengine.IndexUser", "bleveengine.index_user.error", nil, err.Error(), http.StatusInternalServerError)
	}
	return nil
}

func (b *BleveEngine) SearchUsersInChannel(teamId, channelId string, restrictedToChannels []string, term string, options *model.UserSearchOptions) ([]string, []string, *model.AppError) {
	if restrictedToChannels != nil && len(restrictedToChannels) == 0 {
		return []string{}, []string{}, nil
	}

	var queries []query.Query
	if term != "" {
		termQ := bleve.NewPrefixQuery(strings.ToLower(term))
		if options.AllowFullNames {
			termQ.SetField("SuggestionsWithFullname")
		} else {
			termQ.SetField("SuggestionsWithoutFullname")
		}
		queries = append(queries, termQ)
	}

	channelIdQ := bleve.NewTermQuery(channelId)
	channelIdQ.SetField("ChannelsIds")
	queries = append(queries, channelIdQ)

	query := bleve.NewConjunctionQuery(queries...)

	uchan, err := b.userIndex.Search(bleve.NewSearchRequest(query))
	if err != nil {
		return nil, nil, model.NewAppError("Bleveengine.SearchUsersInChannel", "bleveengine.search_users_in_channel.uchan.error", nil, err.Error(), http.StatusInternalServerError)
	}

	boolQ := bleve.NewBooleanQuery()

	if term != "" {
		termQ := bleve.NewPrefixQuery(strings.ToLower(term))
		if options.AllowFullNames {
			termQ.SetField("SuggestionsWithFullname")
		} else {
			termQ.SetField("SuggestionsWithoutFullname")
		}
		boolQ.AddMust(termQ)
	}

	teamIdQ := bleve.NewTermQuery(teamId)
	teamIdQ.SetField("TeamsIds")
	boolQ.AddMust(teamIdQ)

	outsideChannelIdQ := bleve.NewTermQuery(channelId)
	outsideChannelIdQ.SetField("ChannelsIds")
	boolQ.AddMustNot(outsideChannelIdQ)

	if len(restrictedToChannels) > 0 {
		restrictedChannelsQ := bleve.NewDisjunctionQuery()
		for _, channelId := range restrictedToChannels {
			restrictedChannelQ := bleve.NewTermQuery(channelId)
			restrictedChannelsQ.AddQuery(restrictedChannelQ)
		}
		boolQ.AddMust(restrictedChannelsQ)
	}

	nuchan, err := b.userIndex.Search(bleve.NewSearchRequest(boolQ))
	if err != nil {
		return nil, nil, model.NewAppError("Bleveengine.SearchUsersInChannel", "bleveengine.search_users_in_channel.nuchan.error", nil, err.Error(), http.StatusInternalServerError)
	}

	uchanIds := []string{}
	for _, result := range uchan.Hits {
		uchanIds = append(uchanIds, result.ID)
	}

	nuchanIds := []string{}
	for _, result := range nuchan.Hits {
		nuchanIds = append(nuchanIds, result.ID)
	}

	return uchanIds, nuchanIds, nil
}

func (b *BleveEngine) SearchUsersInTeam(teamId string, restrictedToChannels []string, term string, options *model.UserSearchOptions) ([]string, *model.AppError) {
	if restrictedToChannels != nil && len(restrictedToChannels) == 0 {
		return []string{}, nil
	}

	var rootQ query.Query
	if term == "" && teamId == "" && restrictedToChannels == nil {
		rootQ = bleve.NewMatchAllQuery()
	} else {
		boolQ := bleve.NewBooleanQuery()

		if term != "" {
			termQ := bleve.NewPrefixQuery(strings.ToLower(term))
			if options.AllowFullNames {
				termQ.SetField("SuggestionsWithFullname")
			} else {
				termQ.SetField("SuggestionsWithoutFullname")
			}
			boolQ.AddMust(termQ)
		}

		if len(restrictedToChannels) > 0 {
			// restricted channels are already filtered by team, so we
			// can search only those matches
			restrictedChannelsQ := []query.Query{}
			for _, channelId := range restrictedToChannels {
				channelIdQ := bleve.NewTermQuery(channelId)
				channelIdQ.SetField("ChannelsIds")
				restrictedChannelsQ = append(restrictedChannelsQ, channelIdQ)
			}
			boolQ.AddMust(bleve.NewDisjunctionQuery(restrictedChannelsQ...))
		} else {
			// this means that we only need to restrict by team
			if teamId != "" {
				teamIdQ := bleve.NewTermQuery(teamId)
				teamIdQ.SetField("TeamsIds")
				boolQ.AddMust(teamIdQ)
			}
		}

		rootQ = boolQ
	}

	search := bleve.NewSearchRequest(rootQ)

	results, err := b.userIndex.Search(search)
	if err != nil {
		return nil, model.NewAppError("Bleveengine.SearchUsersInTeam", "bleveengine.search_users_in_team.error", nil, err.Error(), http.StatusInternalServerError)
	}

	usersIds := []string{}
	for _, r := range results.Hits {
		usersIds = append(usersIds, r.ID)
	}

	return usersIds, nil
}

func (b *BleveEngine) DeleteUser(user *model.User) *model.AppError {
	if err := b.userIndex.Delete(user.Id); err != nil {
		return model.NewAppError("Bleveengine.DeleteUser", "bleveengine.delete_user.error", nil, err.Error(), http.StatusInternalServerError)
	}
	return nil
}

func (b *BleveEngine) TestConfig(cfg *model.Config) *model.AppError {
	mlog.Warn("TestConfig Bleve")
	return nil
}

func (b *BleveEngine) PurgeIndexes() *model.AppError {
	mlog.Warn("PurgeIndexes Bleve")
	return nil
}

func (b *BleveEngine) DataRetentionDeleteIndexes(cutoff time.Time) *model.AppError {
	return nil
}

func (b *BleveEngine) IsAutocompletionEnabled() bool {
	return *b.cfg.BleveSettings.EnableAutocomplete
}

func (b *BleveEngine) IsIndexingEnabled() bool {
	return *b.cfg.BleveSettings.EnableIndexing
}

func (b *BleveEngine) IsSearchEnabled() bool {
	return *b.cfg.BleveSettings.EnableSearching
}

func (b *BleveEngine) UpdateConfig(cfg *model.Config) {
	if !b.IsActive() && *cfg.BleveSettings.EnableIndexing && *cfg.BleveSettings.IndexDir != "" {
		b.Stop()
		b.cfg = cfg
		b.Start()
		return
	}

	b.cfg = cfg
}
