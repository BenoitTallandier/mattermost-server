package bleveengine

import (
	"strings"

	"github.com/mattermost/mattermost-server/v5/model"
	"github.com/mattermost/mattermost-server/v5/services/searchengine"
)

type BLVChannel struct {
	Id          string
	TeamId      []string
	NameSuggest []string
}

type BLVUser struct {
	Id                         string
	SuggestionsWithFullname    []string
	SuggestionsWithoutFullname []string
	TeamsIds                   []string
	ChannelsIds                []string
}

type BLVPost struct {
	Id          string
	TeamId      string
	ChannelId   string
	UserId      string
	CreateAt    int64
	Message     string
	Type        string
	Hashtags    []string
	Attachments string
}

func BLVChannelFromChannel(channel *model.Channel) *BLVChannel {
	displayNameInputs := searchengine.GetSuggestionInputsSplitBy(channel.DisplayName, " ")
	nameInputs := searchengine.GetSuggestionInputsSplitByMultiple(channel.Name, []string{"-", "_"})

	return &BLVChannel{
		Id:          channel.Id,
		TeamId:      []string{channel.TeamId},
		NameSuggest: append(displayNameInputs, nameInputs...),
	}
}

func BLVUserFromUserAndTeams(user *model.User, teamsIds, channelsIds []string) *BLVUser {
	usernameSuggestions := searchengine.GetSuggestionInputsSplitByMultiple(user.Username, []string{".", "-", "_"})

	fullnameStrings := []string{}
	if user.FirstName != "" {
		fullnameStrings = append(fullnameStrings, user.FirstName)
	}
	if user.LastName != "" {
		fullnameStrings = append(fullnameStrings, user.LastName)
	}

	fullnameSuggestions := []string{}
	if len(fullnameStrings) > 0 {
		fullname := strings.Join(fullnameStrings, " ")
		fullnameSuggestions = searchengine.GetSuggestionInputsSplitBy(fullname, " ")
	}

	nicknameSuggesitons := []string{}
	if user.Nickname != "" {
		nicknameSuggesitons = searchengine.GetSuggestionInputsSplitBy(user.Nickname, " ")
	}

	usernameAndNicknameSuggestions := append(usernameSuggestions, nicknameSuggesitons...)

	return &BLVUser{
		Id:                         user.Id,
		SuggestionsWithFullname:    append(usernameAndNicknameSuggestions, fullnameSuggestions...),
		SuggestionsWithoutFullname: usernameAndNicknameSuggestions,
		TeamsIds:                   teamsIds,
		ChannelsIds:                channelsIds,
	}
}

func BLVPostFromPost(post *model.Post, teamId string) *BLVPost {
	return &BLVPost{
		Id:        post.Id,
		TeamId:    teamId,
		ChannelId: post.ChannelId,
		UserId:    post.UserId,
		CreateAt:  post.CreateAt,
		Message:   post.Message,
		Type:      post.Type,
		Hashtags:  strings.Fields(post.Hashtags),
	}
}
