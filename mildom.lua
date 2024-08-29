local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https?://cloudac%-cf%-jp%.mildom%.com/nonolive/gappserv/user/profileV2%?__platform=web&user_id=([0-9]+)$"]="profile",
    ["^https?://cloudac%-cf%-jp%.mildom%.com/nonolive/videocontent/clip/detail?__platform=web&clip_id=([^&]+)$"]="clip",
    ["^https?://([^/]*mildom%.tv/.+)$"]="asset",
    ["^https?://([^/]*mildom%.com/assets/.+)$"]="asset",
    ["^https?://([^/]*mildom%.com/static/.+)$"]="asset",
    ["^https?://([^/]*mildom%.com/download/.+)$"]="asset"
  }) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      context = {}
      if item_type == "asset" and (
        string.match(url, "%.m3u8$")
        or string.match(url, "%.m3u8%?")
      ) then
        context["m3u8"] = true
      end
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      is_new_design = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  local skip = false
  for pattern, type_ in pairs({
    ["^https?://www.mildom.com/playback/[0-9]+/([0-9a-zA-Z%-]+)$"]="video",
    ["^https?://www.mildom.com/clip/([0-9a-zA-Z%-]+)$"]="clip",
    ["^https?://([^/]*mildom%.tv/.+)$"]="asset",
    ["^https?://([^/]*mildom%.com/assets/.+)$"]="asset",
    ["^https?://([^/]*mildom%.com/static/.+)$"]="asset",
    ["^https?://([^/]*mildom%.com/download/.+)$"]="asset",
  }) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*mildom%.tv/")
    and not string.match(url, "^https?://[^/]*mildom%.com/") then
    discover_item(discovered_outlinks, url)
    return false
  end

  for _, pattern in pairs({
    "([a-z0-9A-Z%-]+)",
    "https?://(.+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      if context["m3u8"] then
        headers["Referer"] = ""
      end
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  if allowed(url)
    and status_code < 300
    and (
      item_type ~= "asset"
      or (
        context["m3u8"]
        and (
          string.match(url, "%.m3u8$")
          or string.match(url, "%.m3u8%?")
        )
      )
    ) then
    html = read_file(file)
    if context["m3u8"]
      and (
        string.match(url, "%.m3u8$")
        or string.match(url, "%.m3u8%?")
      ) then
      for line in string.gmatch(html, "([^\n]+)") do
        if not string.match(line, "^#") then
          local newurl = urlparse.absolute(url, line)
          ids[newurl] = true
          check(newurl)
        end
      end
    end
    if string.match(url, "^https?://cloudac%-cf%-jp%.mildom%.com/") then
      json = cjson.decode(html)
      if string.match(url, "/nonolive/gappserv/user/profileV2%?")
        and json["code"] == 1
        and json["message"] == "" then
        return urls
      end
      if string.match(url, "[%?&]page=")
        and json
        and not string.match(url, "/nonolive/gappserv/index/anchorRecommendV2")
        and not string.match(url, "/nonolive/gappserv/channel/liveEndRecoV4") then
        local body = json["body"]
        local count = 0
        local list_body = body
        for _, key in pairs({"result", "posts"}) do
          if body[key] then
            list_body = body[key]
            break
          end
        end
        count = 0
        if list_body ~= cjson.null then
          for _ in pairs(list_body) do
            count = count + 1
          end
        end
        if count > 1 then
          check(increment_param(url, "page", "1", 1))
        end
      end
    end
    if item_type == "profile" then
      if string.match(url, "/nonolive/gappserv/user/profileV2") then
        check("https://cloudac-cf-jp.mildom.com/nonolive/activityserv/diyActivity/badgeIcon?__platform=web&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/activityserv/enterRoom?__platform=web&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/comet/ws/dispatcher?__platform=web&rid=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/anchor_groups/status?__platform=web&room_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/channel/liveEndRecoV4?__platform=web&room_id=" .. item_value .. "&channel_key=&page=1&limit=4")
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/emotion/getListV1?__platform=web&room_id=" .. item_value .. "&channel_key=")
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/gift/rankV3?__platform=web&user_id=" .. item_value .. "&type=0&limit=3&version=1")
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/index/anchorRecommendV2?__platform=web&page=1&limit=6&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/live/enterstudio?__platform=web&user_id=" .. item_value .. "&source=&sub_source=&mark=1")
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/live/enterstudio?__platform=web&user_id=" .. item_value .. "&source=others&sub_source=&mark=1")
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/paidMessage/getConfig?__platform=web&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/paidMessage/getStickyMessage?__platform=web&room_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/realtimeTag/info?__platform=web&room_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/room/anchorRecommend?__platform=web&room_id=" .. item_value .. "&channel_key=")
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/stream/request/status?&__platform=web&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/user/profileV2?__platform=web&user_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/giftserv/together/gift/info?__platform=web&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/icewolf/interactive/ws/dispatcher?__platform=web&room_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/interactionserv/enterRoom?__platform=web&host_id=" .. item_value .. "&channel_key=&live_type=2&draw_guess_version=1&game_key=")
        check("https://cloudac-cf-jp.mildom.com/nonolive/interactionserv/giftWall/detail?__platform=web&last_week=0&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/interactionserv/giftWall/detail?__platform=web&last_week=0&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/rankserv/gift/rankV3/whitelist?__platform=web&host_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/videocontent/clip/list?__platform=web&user_id=" .. item_value .. "&content_type=0&limit=30&page=1")
        check("https://cloudac-cf-jp.mildom.com/nonolive/videocontent/clip/others/switch?__platform=web&user_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/videocontent/profile/playbackList?__platform=web&user_id=" .. item_value .. "&limit=30&page=1")
        for tab_type=1,8 do
          check("https://cloudac-cf-jp.mildom.com/postbarserve/post/user/list/v2?__platform=web&user_id=" .. item_value .. "&page=1&size=10&tab_type=" .. tostring(tab_type))
        end
        check("https://im.mildom.com/?room_id=" .. item_value .. "&type=chat&call=get_server")
        check("https://www.mildom.com/" .. item_value)
        check("https://www.mildom.com/profile/" .. item_value)
        check("https://www.mildom.com/profile/12046405/gift_wall")
        check("https://www.mildom.com/profile/12046405/timeline")

        -- from video
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/user/findOneV2?_platform=web&user_id=" .. item_value)
        check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/emotion/getListV1?__platform=web&room_id=" .. item_value .. "&channel_key=")
      elseif string.match(url, "/nonolive/videocontent/profile/playbackList") then
        for _, d in pairs(json["body"]) do
          check("https://www.mildom.com/playback/" .. d["user_id"] .. "/" .. d["v_id"])
        end
      elseif string.match(url, "/nonolive/videocontent/clip/list") then
        if json["body"]["result"] then
          for _, d in pairs(json["body"]["result"]) do
            check("https://www.mildom.com/clip/" .. d["clip_id"])
          end
        end
      end
    elseif item_type == "clip"
      and string.match(url, "/nonolive/videocontent/clip/detail") then
      check("https://cloudac-cf-jp.mildom.com/nonolive/videocontent/video/viewInc?__platform=web&v_type=1&v_id=" .. item_value .. "&user_id=" .. json["body"]["user_id"])
      check("https://www.mildom.com/clip/" .. item_value)
    elseif item_type == "video"
      and string.match(url, "/nonolive/videocontent/playback/getPlaybackDetail") then
      local user_id = json["body"]["playback"]["user_id"]
      check("https://www.mildom.com/playback/" .. user_id .. "/" .. item_value)
      check("https://cloudac-cf-jp.mildom.com/nonolive/videocontent/clip/playbackPageList?__platform=web&v_id=" .. item_value .. "&limit=30&page=1")
      check("https://cloudac-cf-jp.mildom.com/nonolive/gappserv/fansGroup/getPageInfo?__platform=web&host_id=" .. user_id .. "&video_type=2&video_id=" .. item_value)
      check("https://cloudac-cf-jp.mildom.com/nonolive/videocontent/playback/getPlaybackDetail?__platform=web&v_id=" .. item_value .. "&mark=1")
    end
    if json then
      html = html .. " " .. flatten_json(json)
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  is_new_design = false
  if http_stat["len"] == 0 then
    retry_url = true
    return false
  end
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 404 then
    retry_url = true
    return false
  end
  if string.match(url["url"], "^https?://cloudac%-cf%-jp%.mildom%.com/.") then
    local html = read_file(http_stat["local_file"])
    if not (
        string.match(html, "^%s*{")
        and string.match(html, "}%s*$")
      ) then
      print("Did not get JSON data.")
      retry_url = true
      return false
    end
    local json = cjson.decode(percent_encode_url(decode_codepoint(html)))
    if json["code"] ~= 0
      and not string.match(url["url"], "/nonolive/gappserv/live/enterstudio")
      and not (
        string.match(url["url"], "/nonolive/gappserv/user/profileV2%?")
        and json["code"] == 1
        and json["message"] == ""
      ) then
      print("Bad response code in JSON.")
      retry_url = true
      return false
    end
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end
  
  if is_new_design then
    return wget.actions.EXIT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if seen_200[url["url"]] then
    print("Received data incomplete.")
    abort_item()
    return wget.actions.EXIT
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 5
    if string.match(url["url"], "^https?://cloudac%-cf%-jp%.mildom%.com/nonolive/gappserv/user/profileV2%?__platform=web&user_id=[0-9]+$") then
      maxtries = 0
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      seen_200[url["url"]] = true
    end
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["mildom-b3vodnlhxn4x0ry4"] = discovered_items,
    ["urls-2or56r5x4qyp2x3c"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


