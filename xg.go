package xinge

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type message struct {
	params map[string]interface{}
	ty     messageType
}

type xg struct {
	protocol                      string
	host                          string
	httpHeader                    http.Header
	accessId                      string
	secretKey                     string
	lastAllDeviceRequestTimestamp int64
}

type TagTokenPair struct {
	Tag   string
	Token string
}

type customResponse struct {
	RetCode int         `json:"ret_code"`
	ErrMsg  string      `json:"err_msg"`
	Result  interface{} `json:"result"`
}

type pushSinglePushIdResponse struct {
	PushId string `json:"push_id"`
}

type pushAccountListResponse map[string]int

type queryAppTagsResponse struct {
	Total int      `json:"total"`
	Tags  []string `json:"tags"`
}

type tokenList struct {
	Tokens []string `json:"tokens"`
}

type tagList struct {
	Tags []string `json:"tags"`
}

type msgStatus struct {
	PushId    string `json:"push_id"`
	Status    int    `json:"status"`
	StartTime string `json:"start_time"`
	Finished  int    `json:"finished"`
	Total     int    `json:"total"`
}

type appDeviceNumResponse struct {
	DeviceNum int64 `json:"device_num"`
}

type appTokenInfo struct {
	IsReg         int   `json:"isReg"`
	ConnTimestamp int64 `json:"connTimestamp"`
	MsgsNum       int   `json:"msgsNum"`
}

type RequestPath struct {
	apiVersion, class, method string
}

type MsgAcceptTime struct {
	Hour string `json:"hour"`
	Min  string `json:"min"`
}

type Aps struct {
	Alert string `json:"alert"`
	Badge int    `json:"badge"`
	Sound string `json:"sound"`
}

type MsgAndroidAction struct {
	ActionType int    `json:"action_type"`
	Activity   string `json:"activity"`
	AtyAttr    struct {
		If int `json:"if"`
		Pf int `json:"pf"`
	} `json:"aty_attr"`
	Browser struct {
		Url     string `json:"url"`
		Confirm int    `json:"confirm"`
	} `json:"browser"`
	Intent string `json:"intent"`
}

type messageType int

type requestMethod string

type messageStuffMethod int

type msgProfilerType string

type tagOp string

const (
	MESSAGE_TYPE_IOS_DEVELOP          messageType = 0
	MESSAGE_TYPE_IOS_PRODUCT                      = 1
	MESSAGE_TYPE_ANDROID_NOTIFICATION             = 2
	MESSAGE_TYPE_ANDROID_PASSTHROUGH              = 3

	SEND_TIME_LAYOUT = "2006-01-02 15:04:05"

	POST requestMethod = "POST"
	GET                = "GET"

	TAG_OP_AND tagOp = "AND"
	TAG_OP_OR        = "OR"

	REMOTE_CALL_SUCCESS                 = 0
	REMOTE_CALL_PARAM_ERR               = -1
	REMOTE_CALL_TIME_OUT_OF_RANGE       = -2
	REMOTE_CALL_SIGN_ILLEGAL            = -3
	REMOTE_CALL_PARAM_ERR_2             = 2
	REMOTE_CALL_TOKEN_ILLEGAL           = 14
	REMOTE_CALL_LOGIC_SERVER_BUSY       = 15
	REMOTE_CALL_OPTION_SEQUENTIAL_ERROR = 19
	REMOTE_CALL_ACCESS_ERROR            = 20
	REMOTE_CALL_TOKEN_NOT_FOUND         = 40
	REMOTE_CALL_ACCOUNT_NO_TOKEN        = 48
	REMOTE_CALL_TAG_SERVER_BUSY         = 63
	REMOTE_CALL_APNS_BUSY               = 71
	REMOTE_CALL_CHAR_TOO_LONG           = 73
	REMOTE_CALL_REQUEST_TOO_FREQUENT    = 76
	REMOTE_CALL_LOOP_TASK_PARAM_ERROR   = 78
	REMOTE_CALL_APNS_ERROR              = 100

	MESSAGE_STUFF_METHOD_STANDARD messageStuffMethod = 1
	MESSAGE_STUFF_METHOD_SIMPLY                      = 2

	MESSAGE_PROFILER_ALLOW_KEY     msgProfilerType = "allow_key"
	MESSAGE_PROFILER_NOT_ALLOW_KEY                 = "not_allow_key"
	MESSAGE_PROFILER_KEY_ALLOW_ADD                 = "key_allow_add"
	MESSAGE_PROFILER_MUST_KEY                      = "must_key"
	MESSAGE_PROFILER_KEY_INT                       = "key_int"
	MESSAGE_PROFILER_KEY_STR                       = "key_str"

	MULTI_PUSH_TOKEN_MAX_LENGTH   = 1000
	PUSH_ACCOUNT_LIST_MAX_LENGTH  = 100
	MUTIL_PUSH_ACCOUNT_MAX_LENGTH = 1000

	TAG_BATCH_SET_MAX_PAIR = 20
	TAG_MAX_LENGTH         = 50
	TOKEN_MIN_LENGTH       = 40

	ALL_DEVICE_REQUEST_MIN_INTERVAL_SECOND = 3
)

var (
	push2AllDeviceRequestPath                    = RequestPath{"v2", "push", "all_device"} //push to all device request path
	push2SingleDeviceRequestPath                 = RequestPath{"v2", "push", "single_device"}
	pushCreateMultiPushRequestPath               = RequestPath{"v2", "push", "create_multipush"}
	push2DeviceListMultipleRequestpath           = RequestPath{"v2", "push", "device_list_multiple"}
	push2TagDeviceRequestPath                    = RequestPath{"v2", "push", "tags_device"}
	push2SingleAccountRequestPath                = RequestPath{"v2", "push", "single_account"}
	push2AccountListRequestPath                  = RequestPath{"v2", "push", "account_list"}
	push2AccountListMutipleRequestPath           = RequestPath{"v2", "push", "account_list_multiple"}
	tagsBatchSetRequstPath                       = RequestPath{"v2", "tags", "batch_set"}
	tagsBatchDelRequestPath                      = RequestPath{"v2", "tags", "batch_del"}
	applicationDelAppAccountTokensRequestPath    = RequestPath{"v2", "application", "del_app_account_tokens"}
	applicationDelAppAccountAllTokensRequestPath = RequestPath{"v2", "application", "del_app_account_all_tokens"}
	pushGetPushStatusRequestPath                 = RequestPath{"v2", "push", "get_msg_status"}
	applicationGetAppDeviceNumRequestPath        = RequestPath{"v2", "application", "get_app_device_num"}
	applicationGetAppTokenInfoRequestPath        = RequestPath{"v2", "application", "get_app_token_info"}
	applicationGetAppAccountTokensRequestPath    = RequestPath{"v2", "application", "get_app_account_tokens"}
	tagsQueryAppTagsRequestPath                  = RequestPath{"v2", "tag", "query_app_tags"}
	tagsQueryTokenTagsRequestPath                = RequestPath{"v2", "tags", "query_token_tags"}
	tagsQueryTagTokenNumRequestPath              = RequestPath{"v2", "tags", "query_tag_token_num"}
	pushDeleteOfflineMsgRequestPath              = RequestPath{"v2", "push", "delete_offline_msg"}
	pushCancelTimingTaskRequestPath              = RequestPath{"v2", "push", "cancel_timing_task"}

	allDeviceMu sync.Mutex

	messageProfiler = map[messageType]map[msgProfilerType][]string{
		MESSAGE_TYPE_IOS_DEVELOP: {
			MESSAGE_PROFILER_NOT_ALLOW_KEY: {"xg"},
			MESSAGE_PROFILER_MUST_KEY:      {"aps"},
			MESSAGE_PROFILER_KEY_ALLOW_ADD: {"accept_time"},
		},
		MESSAGE_TYPE_IOS_PRODUCT: {
			MESSAGE_PROFILER_NOT_ALLOW_KEY: {"xg"},
			MESSAGE_PROFILER_MUST_KEY:      {"aps"},
			MESSAGE_PROFILER_KEY_ALLOW_ADD: {"accept_time"},
		},
		MESSAGE_TYPE_ANDROID_NOTIFICATION: {
			MESSAGE_PROFILER_ALLOW_KEY:     {"title", "content", "accept_time", "n_id", "builder_id", "ring", "ring_raw", "vibrate", "lights", "clearable", "icon_type", "icon_res", "style_id", "small_icon", "action", "custom_content"},
			MESSAGE_PROFILER_MUST_KEY:      {"title", "content", "builder_id"},
			MESSAGE_PROFILER_KEY_INT:       {"n_id", "builder_id", "ring", "vibrate", "lights", "clearable", "icon_type", "style_id"},
			MESSAGE_PROFILER_KEY_STR:       {"title", "content", "ring_raw", "icon_res", "small_icon"},
			MESSAGE_PROFILER_KEY_ALLOW_ADD: {"accept_time", "custom_content"},
		},
		MESSAGE_TYPE_ANDROID_PASSTHROUGH: {
			MESSAGE_PROFILER_ALLOW_KEY:     {"title", "content", "accept_time", "custom_content"},
			MESSAGE_PROFILER_KEY_STR:       {"title", "content"},
			MESSAGE_PROFILER_KEY_ALLOW_ADD: {"accept_time", "custom_content"},
		},
	}

	errMessageParamAddingNotAllow     = errors.New("message adding not allow")
	errMessageParamSettingNotAllow    = errors.New("message setting not allow")
	errMessageParamValueTypeIllegal   = errors.New("message value type illegal")
	errMessageParamMustKeyNotFound    = errors.New("message must key not found")
	errMessagePushMultiTokenTooMuch   = errors.New("token too much, limit to 1000")
	errMessagePushAccountTooMuch      = errors.New("account too much, limit to 100")
	errMessagePushMultiAccountTooMuch = errors.New("account too much, limit to 1000")

	errTagBatchSetPairTooMuch = errors.New("tag token too much, limit to 20")
	errTagTooLength           = errors.New("tag too long , limit to 50 chars")
	errTokenTooShort          = errors.New("token too short, min 40 chars")

	errReadResponse = errors.New("read response msg error")

	errAllDeviceRequestInterval = errors.New("all device request interval min 3 seconds")
)

func (this RequestPath) Topath() string {
	return this.apiVersion + "/" + this.class + "/" + this.method
}

func (this *message) ToJson() string {
	body, _ := json.Marshal(this.params)
	return string(body)
}

func newMessage(msgType messageType) *message {
	msg := &message{ty: msgType}
	msg.params = make(map[string]interface{})
	return msg
}

func NewSimplyMessage(msgType messageType) *message {
	msg := newMessage(msgType)
	msg.stuffDefault(MESSAGE_STUFF_METHOD_SIMPLY)
	return msg
}

func NewStandardMessage(msgType messageType) *message {
	msg := newMessage(msgType)
	msg.stuffDefault(MESSAGE_STUFF_METHOD_STANDARD)
	return msg
}

func (this *message) stuffDefault(stuffMethod messageStuffMethod) {
	switch stuffMethod {
	case MESSAGE_STUFF_METHOD_SIMPLY:
		this.simplyStuffDefault()
	case MESSAGE_STUFF_METHOD_STANDARD:
		this.standardStuffDefault()
	}
}

func (this *message) simplyStuffDefault() {
	switch this.ty {
	case MESSAGE_TYPE_IOS_DEVELOP:
	case MESSAGE_TYPE_IOS_PRODUCT:
	case MESSAGE_TYPE_ANDROID_NOTIFICATION:
		this.Set("builder_id", 1)
	case MESSAGE_TYPE_ANDROID_PASSTHROUGH:
	}
}

func (this *message) standardStuffDefault() {
	switch this.ty {
	case MESSAGE_TYPE_IOS_DEVELOP:
	case MESSAGE_TYPE_IOS_PRODUCT:
	case MESSAGE_TYPE_ANDROID_NOTIFICATION:
		this.Set("n_id", 0)
		this.Set("builder_id", 0)
		this.Set("ring", 1)
		this.Set("vibrate", 1)
		this.Set("lights", 1)
		this.Set("clearable", 1)
		this.Set("icon_type", 0)
		this.Set("style_id", 1)
	case MESSAGE_TYPE_ANDROID_PASSTHROUGH:
	}
}

func (this *message) Set(key string, val interface{}) error {
	if err := this.checkSettingKey(key, val); err != nil {
		return err
	}
	if err := this.checkParamsType(key, val); err != nil {
		return err
	}

	isArray := false
	if profiler, ok := messageProfiler[this.ty]; ok {
		if param, ok := profiler[MESSAGE_PROFILER_KEY_ALLOW_ADD]; ok {
			for _, k := range param {
				if key == k {
					tmp := []interface{}{}
					tmp = append(tmp, val)
					this.params[key] = tmp
					isArray = !isArray
					break
				}
			}
		}
	}
	if !isArray {
		this.params[key] = val
	}

	return nil

}

func (this *message) Add(key string, val interface{}) error {
	if err := this.checkAddingKey(key, val); err != nil {
		return err
	}
	if err := this.checkParamsType(key, val); err != nil {
		return err
	}
	var tmp []interface{}

	p, ok := this.params[key]
	if !ok {
		tmp = []interface{}{}
	} else {
		v := reflect.ValueOf(p)
		l := v.Len()
		tmp := make([]interface{}, l)
		for i := 0; i < l; i++ {
			tmp[i] = v.Index(i).Interface()
		}
	}
	tmp = append(tmp, val)

	this.params[key] = tmp
	return nil
}

func (this message) checkParamsType(key string, val interface{}) error {

	profiler, ok := messageProfiler[this.ty]
	if !ok {
		return nil
	}
	if ci, ok := profiler[MESSAGE_PROFILER_KEY_INT]; ok {
		if !this.checkInt(ci, key, val) {
			return errMessageParamValueTypeIllegal
		}
	}
	if cs, ok := profiler[MESSAGE_PROFILER_KEY_STR]; ok {
		if !this.checkStr(cs, key, val) {
			return errMessageParamValueTypeIllegal
		}
	}

	switch this.ty {

	case MESSAGE_TYPE_IOS_DEVELOP:
		fallthrough
	case MESSAGE_TYPE_IOS_PRODUCT:
		if !this.checkIOSOtherType(key, val) {
			return errMessageParamValueTypeIllegal
		}
	case MESSAGE_TYPE_ANDROID_NOTIFICATION:
		if !this.checkAndroidNotificationOtherType(key, val) {
			return errMessageParamValueTypeIllegal
		}
	case MESSAGE_TYPE_ANDROID_PASSTHROUGH:
		if !this.checkAndroidPassthroughOtherType(key, val) {
			return errMessageParamValueTypeIllegal
		}
	}
	return nil
}

func (this message) checkInt(checker []string, key string, val interface{}) bool {
	for _, k := range checker {
		if k == key {
			switch val.(type) {
			case int:
			default:
				return false
			}
			break
		}
	}
	return true
}

func (this message) checkStr(checker []string, key string, val interface{}) bool {
	for _, k := range checker {
		if k == key {
			switch val.(type) {
			case string:
			default:
				return false
			}

			break
		}
	}
	return true
}

func (this message) checkIOSOtherType(key string, val interface{}) bool {
	switch key {
	case "aps":
		switch val.(type) {
		case Aps:
		default:
			return false
		}

		return true
	}
	return this.checkMessageCustomOtherType(key, val)
}

func (this message) checkAndroidPassthroughOtherType(key string, val interface{}) bool {
	return this.checkMessageCustomOtherType(key, val)
}

func (this message) checkAndroidNotificationOtherType(key string, val interface{}) bool {

	switch key {
	case "action":
		switch val.(type) {
		case MsgAndroidAction:
		default:
			return false
		}

		return true
	}
	return this.checkMessageCustomOtherType(key, val)
}

func (this message) checkMessageCustomOtherType(key string, val interface{}) bool {
	switch key {
	case "accept_time":
		switch val.(type) {
		case MsgAcceptTime:
		default:
			return false
		}

	}
	return true
}

func (this message) checkSettingKey(key string, val interface{}) error {

	profiler, ok := messageProfiler[this.ty]
	if !ok {
		return nil
	}
	notAllow, ok := profiler[MESSAGE_PROFILER_NOT_ALLOW_KEY]
	if ok {
		for _, not := range notAllow {
			if not == key {
				return errMessageParamSettingNotAllow
			}
		}
	}
	allow, ok := profiler[MESSAGE_PROFILER_ALLOW_KEY]
	if ok {
		p := false
		for _, a := range allow {
			if a == key {
				p = !p
				break
			}
		}
		if !p {
			return errMessageParamSettingNotAllow
		}
	}
	return nil
}

func (this message) checkAddingKey(key string, val interface{}) (err error) {

	profiler, ok := messageProfiler[this.ty]
	if !ok {
		return nil
	}
	addAllow, ok := profiler[MESSAGE_PROFILER_KEY_ALLOW_ADD]
	if !ok {
		return nil
	}
	p := false
	for _, aa := range addAllow {

		if aa == key {
			p = !p
			break
		}

	}
	if p {
		return nil
	} else {
		return errMessageParamAddingNotAllow
	}

}

func (this message) checkMustKey() error {
	profiler, ok := messageProfiler[this.ty]
	if !ok {
		return nil
	}
	mustKey, ok := profiler[MESSAGE_PROFILER_MUST_KEY]
	if !ok {
		return nil
	}
	for _, mk := range mustKey {
		if _, ok := this.params[mk]; !ok {
			return errMessageParamMustKeyNotFound
		}
	}
	return nil
}

func NewXg(accessId, secretKey string) *xg {
	gateway := &xg{
		protocol:   "http",
		host:       "openapi.xg.qq.com",
		httpHeader: http.Header{},
		accessId:   accessId,
		secretKey:  secretKey}
	gateway.httpHeader.Add("Content-type", "application/x-www-form-urlencoded")
	return gateway
}

func (this *xg) encrypt(method requestMethod, path RequestPath, msg map[string]string) string {

	encryptStr := string(method)
	encryptStr += this.host + "/"
	encryptStr += path.Topath()
	keys := []string{}

	for k, _ := range msg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		encryptStr += k + "=" + msg[k]
	}
	encryptStr += this.secretKey
	return fmt.Sprintf("%x", md5.Sum([]byte(encryptStr)))
}

func (this xg) customParam() (msg map[string]string) {
	msg = make(map[string]string)
	msg["access_id"] = this.accessId
	msg["timestamp"] = strconv.FormatInt(time.Now().Unix(), 10)
	msg["valid_time"] = "600"
	return msg
}

func (this xg) stuffMsgCustomParam(msg *message, kvMsg *map[string]string) {
	switch msg.ty {
	case MESSAGE_TYPE_IOS_DEVELOP:
		(*kvMsg)["message_type"] = "0"
		(*kvMsg)["environment"] = "2"
		(*kvMsg)["multi_pkg"] = "0"
	case MESSAGE_TYPE_IOS_PRODUCT:
		(*kvMsg)["message_type"] = "0"
		(*kvMsg)["environment"] = "1"
		(*kvMsg)["multi_pkg"] = "0"
	case MESSAGE_TYPE_ANDROID_NOTIFICATION:
		(*kvMsg)["message_type"] = "1"
		(*kvMsg)["multi_pkg"] = "0"
		(*kvMsg)["environment"] = "0"
	case MESSAGE_TYPE_ANDROID_PASSTHROUGH:
		(*kvMsg)["message_type"] = "2"
		(*kvMsg)["multi_pkg"] = "0"
		(*kvMsg)["environment"] = "0"
	}
	(*kvMsg)["message"] = msg.ToJson()
	(*kvMsg)["expire_time"] = "0"

}

func (this *xg) Push2SingleDevice(msg *message, deviceToken string) (err error) {
	if err = msg.checkMustKey(); err != nil {
		return
	}
	kvMsg := this.customParam()
	this.stuffMsgCustomParam(msg, &kvMsg)
	kvMsg["device_token"] = deviceToken
	kvMsg["send_time"] = this.exchangeSendTime2Str(time.Now().Unix())
	_, err = this.request(POST, push2SingleDeviceRequestPath, kvMsg)
	return
}

func (this *xg) createMultiPushTask(msg *message) (pushId string, err error) {
	multiKvMsg := this.customParam()
	this.stuffMsgCustomParam(msg, &multiKvMsg)
	data, err := this.request(POST, pushCreateMultiPushRequestPath, multiKvMsg)
	if err != nil {
		return
	}
	pushId, err = this.data2SinglePushIdResponse(data)
	return
}

func (this *xg) Push2MultiDevice(msg *message, tokens []string) (pushId string, err error) {
	if len(tokens) > MULTI_PUSH_TOKEN_MAX_LENGTH {
		err = errMessagePushMultiTokenTooMuch
		return
	}
	if err = msg.checkMustKey(); err != nil {
		return
	}

	pushId, err = this.createMultiPushTask(msg)
	if err != nil {
		return
	}

	pushListKvMsg := this.customParam()
	tokensJson, err := json.Marshal(tokens)
	if err != nil {
		pushId = ""
		return
	}
	pushListKvMsg["device_list"] = string(tokensJson)
	pushListKvMsg["push_id"] = pushId
	_, err = this.request(POST, push2DeviceListMultipleRequestpath, pushListKvMsg)
	return
}

func (this *xg) Push2AllDevice(msg *message) (pushId string, err error) {
	allDeviceMu.Lock()
	defer allDeviceMu.Unlock()
	if this.lastAllDeviceRequestTimestamp != 0 && time.Now().Unix()-this.lastAllDeviceRequestTimestamp <= ALL_DEVICE_REQUEST_MIN_INTERVAL_SECOND {
		err = errAllDeviceRequestInterval
		return
	}
	if err = msg.checkMustKey(); err != nil {
		return
	}
	kvMsg := this.customParam()
	this.stuffMsgCustomParam(msg, &kvMsg)
	// kvMsg["loop_times"] = "1"
	// kvMsg["loop_interval"] = "1"
	kvMsg["send_time"] = this.exchangeSendTime2Str(time.Now().Unix())
	data, err := this.request(POST, push2AllDeviceRequestPath, kvMsg)

	this.lastAllDeviceRequestTimestamp = time.Now().Unix()
	if err != nil {
		return
	}
	pushId, err = this.data2SinglePushIdResponse(data)

	return
}

func (this *xg) Push2TagsDevice(msg *message, tagList []string, op tagOp) (pushId string, err error) {
	if err = msg.checkMustKey(); err != nil {
		return
	}
	kvMsg := this.customParam()
	this.stuffMsgCustomParam(msg, &kvMsg)
	tagsJson, err := json.Marshal(tagList)
	if err != nil {
		return
	}
	kvMsg["tags_list"] = string(tagsJson)
	kvMsg["tags_op"] = string(op)
	kvMsg["loop_times"] = "1"
	kvMsg["loop_interval"] = "1"
	kvMsg["send_time"] = this.exchangeSendTime2Str(time.Now().Unix())
	data, err := this.request(POST, push2TagDeviceRequestPath, kvMsg)
	if err != nil {
		return
	}
	pushId, err = this.data2SinglePushIdResponse(data)

	return
}

func (this *xg) Push2SingleAccount(msg *message, account string) (err error) {
	if err = msg.checkMustKey(); err != nil {
		return
	}
	kvMsg := this.customParam()
	this.stuffMsgCustomParam(msg, &kvMsg)
	kvMsg["account"] = account
	kvMsg["send_time"] = this.exchangeSendTime2Str(time.Now().Unix())
	_, err = this.request(POST, push2SingleAccountRequestPath, kvMsg)
	return

}

func (this *xg) Push2AccountList(msg *message, accountList []string) (accountResult *pushAccountListResponse, err error) {
	if len(accountList) > PUSH_ACCOUNT_LIST_MAX_LENGTH {
		err = errMessagePushAccountTooMuch
		return
	}
	if err = msg.checkMustKey(); err != nil {
		return
	}
	kvMsg := this.customParam()
	this.stuffMsgCustomParam(msg, &kvMsg)
	accountJson, err := json.Marshal(accountList)
	if err != nil {
		return
	}
	kvMsg["account_list"] = string(accountJson)
	data, err := this.request(POST, push2SingleAccountRequestPath, kvMsg)
	if err != nil {
		return
	}
	accountResult = &pushAccountListResponse{}
	err = json.Unmarshal([]byte(data), accountResult)
	return
}

func (this *xg) Push2MultiAccount(msg *message, accountList []string) (pushId string, err error) {
	if len(accountList) > MUTIL_PUSH_ACCOUNT_MAX_LENGTH {
		err = errMessagePushMultiAccountTooMuch
		return
	}
	if err = msg.checkMustKey(); err != nil {
		return
	}

	pushId, err = this.createMultiPushTask(msg)
	if err != nil {
		return
	}

	pushListKvMsg := this.customParam()
	accountJson, err := json.Marshal(accountList)
	if err != nil {
		pushId = ""
		return
	}
	pushListKvMsg["account_list"] = string(accountJson)
	pushListKvMsg["push_id"] = pushId
	_, err = this.request(POST, push2AccountListMutipleRequestPath, pushListKvMsg)
	return
}

func (this *xg) tagBatchOp(tagTokenList []TagTokenPair, path RequestPath) (err error) {
	if len(tagTokenList) > TAG_BATCH_SET_MAX_PAIR {
		err = errTagBatchSetPairTooMuch
		return
	}
	data := [][]string{}
	for _, ttp := range tagTokenList {
		if len(ttp.Tag) > TAG_MAX_LENGTH {
			err = errTagTooLength
			return
		}
		if len(ttp.Token) < TOKEN_MIN_LENGTH {
			err = errTokenTooShort
			return
		}
		td := []string{}
		td = append(td, ttp.Tag)
		td = append(td, ttp.Token)
		data = append(data, td)
	}

	kvMsg := this.customParam()
	tagTokenJson, err := json.Marshal(data)
	if err != nil {
		return
	}
	kvMsg["tag_token_list"] = string(tagTokenJson)
	_, err = this.request(POST, path, kvMsg)
	return
}

func (this *xg) TagBatchSet(tagTokenList []TagTokenPair) (err error) {
	err = this.tagBatchOp(tagTokenList, tagsBatchSetRequstPath)
	return
}

func (this *xg) TagBatchDel(tagTokenList []TagTokenPair) (err error) {
	err = this.tagBatchOp(tagTokenList, tagsBatchDelRequestPath)
	return
}

func (this *xg) DelAppAccountToken(account, token string) (leftTokens []string, err error) {
	kvMsg := this.customParam()

	kvMsg["account"] = account
	kvMsg["device_token"] = token
	data, err := this.request(POST, applicationDelAppAccountTokensRequestPath, kvMsg)
	if err != nil {
		return
	}
	response := &tokenList{}
	if err = json.Unmarshal([]byte(data), response); err != nil {
		return
	}
	leftTokens = response.Tokens
	return
}

func (this *xg) DelAppAccountAllTokens(account string) (err error) {
	kvMsg := this.customParam()
	kvMsg["account"] = account
	_, err = this.request(POST, applicationDelAppAccountAllTokensRequestPath, kvMsg)
	return
}

func (this *xg) GetMsgStatus(pushIds []string) (status map[string]msgStatus, err error) {

	data := []map[string]string{}
	for _, pushId := range pushIds {
		tp := make(map[string]string)
		tp["push_id"] = pushId
		data = append(data, tp)
	}
	dataJson, err := json.Marshal(data)
	if err != nil {
		return
	}
	kvMsg := this.customParam()
	kvMsg["push_id"] = string(dataJson)
	rdata, err := this.request(POST, pushGetPushStatusRequestPath, kvMsg)
	if err != nil {
		return
	}
	tmp := map[string][]msgStatus{}
	if err = json.Unmarshal([]byte(rdata), tmp); err != nil {
		return
	}
	status = make(map[string]msgStatus)
	s, ok := tmp["list"]
	if !ok {
		err = errReadResponse
		return
	}
	for _, st := range s {
		status[st.PushId] = st
	}
	return
}

func (this *xg) GetAppDeviceNum() (num int64, err error) {
	kvMsg := this.customParam()
	data, err := this.request(POST, applicationGetAppDeviceNumRequestPath, kvMsg)
	if err != nil {
		return
	}
	response := &appDeviceNumResponse{}
	if err = json.Unmarshal([]byte(data), response); err != nil {
		return
	}
	num = response.DeviceNum
	return
}

func (this *xg) GetAppTokenInfo(token string) (info *appTokenInfo, err error) {
	kvMsg := this.customParam()
	kvMsg["device_token"] = token
	data, err := this.request(POST, applicationGetAppTokenInfoRequestPath, kvMsg)
	if err != nil {
		return
	}
	info = &appTokenInfo{}
	err = json.Unmarshal([]byte(data), info)
	return
}

func (this *xg) GetAppAccountTokens(account string) (tokens []string, err error) {
	kvMsg := this.customParam()
	kvMsg["account"] = account
	data, err := this.request(POST, applicationGetAppAccountTokensRequestPath, kvMsg)
	if err != nil {
		return
	}
	response := &tokenList{}
	if err = json.Unmarshal([]byte(data), response); err != nil {
		return
	}
	tokens = response.Tokens
	return
}

func (this *xg) QueryAppTags(start, limit int) (info *queryAppTagsResponse, err error) {
	kvMsg := this.customParam()
	kvMsg["start"] = strconv.Itoa(start)
	kvMsg["limit"] = strconv.Itoa(limit)
	data, err := this.request(POST, tagsQueryAppTagsRequestPath, kvMsg)
	if err != nil {
		return
	}
	info = &queryAppTagsResponse{}
	err = json.Unmarshal([]byte(data), info)
	return
}

func (this *xg) QueryTokenTags(token string) (tags []string, err error) {
	kvMsg := this.customParam()
	kvMsg["device_token"] = token
	data, err := this.request(POST, tagsQueryTokenTagsRequestPath, kvMsg)
	if err != nil {
		return
	}
	response := &tagList{}
	if err = json.Unmarshal([]byte(data), response); err != nil {
		return
	}
	tags = response.Tags
	return
}

func (this *xg) QueryTagTokenNum(tag string) (num int, err error) {
	kvMsg := this.customParam()
	kvMsg["tag"] = tag
	data, err := this.request(POST, tagsQueryTagTokenNumRequestPath, kvMsg)
	if err != nil {
		return
	}
	response := make(map[string]int)
	if err = json.Unmarshal([]byte(data), response); err != nil {
		return
	}
	num, ok := response["device_num"]
	if !ok {
		err = errReadResponse
		return
	}
	return
}

func (this *xg) DeleteOfflineMsg(pushId string) (err error) {
	kvMsg := this.customParam()
	kvMsg["push_id"] = pushId
	_, err = this.request(POST, pushDeleteOfflineMsgRequestPath, kvMsg)
	return
}

func (this *xg) CancleTimingTask(pushId string) (r bool, err error) {
	kvMsg := this.customParam()
	kvMsg["push_id"] = pushId
	data, err := this.request(POST, pushCancelTimingTaskRequestPath, kvMsg)
	response := make(map[string]int)
	if err = json.Unmarshal([]byte(data), response); err != nil {
		return
	}
	status, ok := response["status"]
	if !ok {
		err = errReadResponse
		return
	}
	if status == 0 {
		r = !r
	}
	return
}

func (this xg) data2SinglePushIdResponse(data string) (pushId string, err error) {
	response := &pushSinglePushIdResponse{}
	err = json.Unmarshal([]byte(data), response)
	if err == nil {
		pushId = response.PushId
	}
	return
}

func (this xg) map2UrlValue(msg map[string]string) url.Values {
	values := make(url.Values)
	for k, m := range msg {
		values.Set(k, m)
	}
	return values
}

func (this *xg) request(method requestMethod, path RequestPath, kvMsg map[string]string) (data string, err error) {
	client := &http.Client{}
	url := this.protocol + "://" + this.host + "/" + path.Topath()
	sign := this.encrypt(method, path, kvMsg)
	kvMsg["sign"] = sign
	values := this.map2UrlValue(kvMsg)

	request, err := http.NewRequest(string(method), url, strings.NewReader(values.Encode()))
	if err != nil {
		return
	}
	request.Header = this.httpHeader
	response, err := client.Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()
	tmp, _ := ioutil.ReadAll(response.Body)

	custom := &customResponse{}
	if err = json.Unmarshal(tmp, custom); err != nil {
		return
	}
	if custom.RetCode != REMOTE_CALL_SUCCESS {
		err = errors.New(strconv.Itoa(custom.RetCode) + " : " + custom.ErrMsg)
		return
	}
	tmpData, _ := json.Marshal(custom.Result)
	data = string(tmpData)
	return
}

func (this xg) exchangeSendTime2Str(sendTime int64) string {
	return time.Unix(sendTime, 0).Format(SEND_TIME_LAYOUT)
}
