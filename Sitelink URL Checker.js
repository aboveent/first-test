function main() {
var errorUrls = [];
function getSitelinksForCampaign() {
var campaignIterator = AdWordsApp.campaigns()
        .withCondition('CampaignStatus IN [ENABLED]')
        .get();
    while (campaignIterator.hasNext()) {
      var campaign = campaignIterator.next();
      var sitelinksIterator = campaign.extensions().sitelinks().get();
    while (sitelinksIterator.hasNext()) {
      var sitelink = sitelinksIterator.next();
      var textSitelink = sitelink.getLinkText();
      var morelinks = sitelink.urls().getFinalUrl();
      if (morelinks === null)
         continue;
      else
   var response = UrlFetchApp.fetch(morelinks.split('?')[0], {followRedirects: false, muteHttpExceptions: true});
   var rescheck = response.getResponseCode();
   var together = rescheck + ' - ' + textSitelink + ' - ' + morelinks;
     if (rescheck === 200) 
      continue;
      else {
     errorUrls.push(together);
     Logger.log(errorUrls);
      } }
}
                if (errorUrls.length > 0) {
       MailApp.sendEmail('example@mindshareworld.com',
                    'Jaguar Brand Sitelink Errors!',
                         'There are sitelink errors! See report below: \n\n' + errorUrls.join('\n\n')); }
}
                getSitelinksForCampaign();
}
