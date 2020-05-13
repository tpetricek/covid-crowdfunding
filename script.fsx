#r "System.ServiceModel.dll"
#load ".paket/load/net45/Deedle.fsx"
#load ".paket/load/net45/FSharp.Data.fsx"
#load ".paket/load/net45/FSharp.Data.TypeProviders.fsx"
#load ".paket/load/net45/Newtonsoft.Json.fsx"
open System
open Deedle
open FSharp.Data
open Newtonsoft.Json
open FSharp.Data.TypeProviders
open System.Net
open System.Web
open System.IO

let temp = __SOURCE_DIRECTORY__ + "/downloads"
Directory.CreateDirectory(temp)

// --------------------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------------------

let downloadCompressed (url:string) = 
  let fn = temp + "/" + HttpUtility.UrlEncode(url)
  if not (File.Exists(fn)) then 
    printfn "Downloading: %s" url
    let req = HttpWebRequest.Create(url) :?> HttpWebRequest
    req.AutomaticDecompression <- DecompressionMethods.Deflate ||| DecompressionMethods.GZip
    use resp = req.GetResponse()
    use sr = new StreamReader(resp.GetResponseStream())
    File.WriteAllText(fn, sr.ReadToEnd())
  File.ReadAllText(fn)

// --------------------------------------------------------------------------------------
// Charity search
// --------------------------------------------------------------------------------------

type CharitySearch = WsdlService<"https://apps.charitycommission.gov.uk/Showcharity/API/SearchCharitiesV1/SearchCharitiesV1.asmx">
type FetchCharity = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/charity.json")>

let chs = CharitySearch.GetSearchCharitiesV1Soap()

let fetchCharity id =   
  let fn = temp + "/charity-" + string id + ".json"
  if not (File.Exists(fn)) then 
    let ch = chs.GetCharityByRegisteredCharityNumber("cf92136a-f3c7-4be1-a", id)
    let js = JsonSerializer.Create()
    let sw = new StringWriter()
    js.Serialize(sw, ch)
    File.WriteAllText(fn, sw.ToString())
  File.ReadAllText(fn) |> FetchCharity.Parse
  
// --------------------------------------------------------------------------------------
// Virgin scraping
// --------------------------------------------------------------------------------------

type VirginFunraisers = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/virgin-fundraisers.json")>
type VirginDonation = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/virgin-donation.json")>

let virginFundraisers term start = 
  try
    sprintf "https://uk.virginmoneygiving.com/fundraiser-display/genericSearch?searchTerm=%s&searchType=FUNDRAISER_ACTIVITY&start=%d"
      term start |> downloadCompressed |> VirginFunraisers.Parse |> Some 
  with _ -> 
    None

let virginFundraiserDetail displ = 
  try 
    ("https://uk.virginmoneygiving.com/fundraiser-display/" + displ) 
      |> downloadCompressed |> HtmlDocument.Parse |> Some
  with _ -> 
    None

let virginDonationPage fid pg =
  (sprintf "https://uk.virginmoneygiving.com/fundraiser-display/getMoreDonationDetails?fundraiserActivityId=%d&pageNumber=%d" fid pg)
    |> downloadCompressed |> VirginDonation.Parse

let virginDonations fid = 
  let rec loop pg = seq {
    let p = virginDonationPage fid pg
    if p.Result.Length > 0 then 
      yield! p.Result
      yield! loop (pg + 1) }
  loop 1

let virginCharityDetail (id:int) = 
  ("https://uk.virginmoneygiving.com/charity-web/charity/finalCharityHomepage.action?charityId=" + string id)
    |> downloadCompressed |> HtmlDocument.Parse 

let rec downloadVirginFundraisers kvd i = seq {
  match virginFundraisers kvd (i * 5) with
  | Some vf -> 
      for f in vf.FundraiserSearchJson.Fundraisers do
        yield f 
      yield! downloadVirginFundraisers kvd (i+1)
  | _ -> () }

let (|CssSelect|) css (fd:HtmlDocument) =
  fd.CssSelect(css)
let (|ByTagAndId|_|) (tag,id) (fd:HtmlDocument) = 
  fd.CssSelect(tag) |> Seq.tryFind (fun i ->
    (i.AttributeValue("id").Trim() = id) )

type Fundraiser = 
  { Fundraiser : string 
    Raised : float 
    Charity : string 
    Operates : string 
    Postcode : string 
    Activities : string 
    Description : string 
    Address : string 
    FirstDonation : string
    LastDonation : string }

let toDateTime (timestamp:int64) =
  let start = DateTime(1970,1,1,0,0,0,DateTimeKind.Utc)
  start.AddSeconds(float (timestamp / 1000L)).ToLocalTime()

// --------------------------------------------------------------------------------------
// Virgin fundraisers
// --------------------------------------------------------------------------------------

//let kvd, fname = "food bank", "food_bank.csv"
//let kvd, fname = "foodbank", "foodbank.csv"
let kvd, fname = "homeless", "homeless.csv"


let fundraisers = ResizeArray<_>()

for f in downloadVirginFundraisers kvd 0 do 
  let p = virginFundraiserDetail f.DisplayPageUrl
  match virginFundraiserDetail f.DisplayPageUrl with 
  | Some 
      ( ByTagAndId ("input", "fundraiserPageActivityId") pageid & 
        ByTagAndId ("input", "totalDonationInputHidden") don ) ->
      let raised = don.Attribute("value").Value() |> float
      let dons = virginDonations (int (pageid.AttributeValue("value"))) 
  
      printfn "\nFUNDRAISER: %s (%s)" f.FundraiserName f.DisplayPageUrl
      printfn "DONATIONS: %A" (Seq.length dons)
      let dates = dons |> Seq.map (fun d -> d.DonationDatetime)
      if not (Seq.isEmpty dons) then
        printfn "FIRST DONATION: %O" (toDateTime (Seq.min dates))
        printfn "LAST DONATION: %O" (toDateTime (Seq.max dates))
        printfn "RAISED: GBP %f" raised

      //for n, id in Seq.zip f.CharityNames f.CharityIds do
      let n, id = Seq.zip f.CharityNames f.CharityIds |> Seq.head // not right
      let ch = fetchCharity id
      printfn "CHARITY: %s" n
      printfn "OPERATES: %s" (ch.AreaOfOperation |> Seq.map (fun c -> c.Trim()) |> String.concat ", ")
      printfn "POSTCODE: %s" ch.Address.Postcode
      printfn "ACTIVITIES: %s" ch.Activities

      let vch = virginCharityDetail id
      let desc = 
        [ for p in vch.CssSelect("#vm-create-event-homepage60-col1 p") |> Seq.tail -> p.InnerText() ]
        |> String.concat "\n"

      let addr = 
        vch.CssSelect(".side-panel-no-bg p") |> Seq.pick (fun p ->
        if p.InnerText().Contains("Registered address") then Some(p.DirectInnerText().Trim().Replace("\n"," ").Replace("\r"," ")) 
        else None)

      printfn "DESCRIPTION: %s" (desc.Replace("\r", " ").Replace("\n", " "))
      printfn "ADDRESS: %s" addr

      { Fundraiser = f.FundraiserName
        Raised = raised
        Charity = n
        FirstDonation = if Seq.isEmpty dates then "" else (toDateTime (Seq.min dates)).ToShortDateString()
        LastDonation = if Seq.isEmpty dates then "" else (toDateTime (Seq.max dates)).ToShortDateString()
        Operates = ch.AreaOfOperation |> Seq.map (fun c -> c.Trim()) |> String.concat ", "
        Postcode = ch.Address.Postcode
        Activities = ch.Activities
        Description = desc
        Address = addr } |> fundraisers.Add
  | _ -> ()

let df = Frame.ofRecords fundraisers
df.SaveCsv(__SOURCE_DIRECTORY__ + "/outputs/" + fname)

// --------------------------------------------------------------------------------------
// GoFundMe
// --------------------------------------------------------------------------------------

type GoFundDonations = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/gofund-donations.json")>

let goFundSearchPage page term country = 
  let raw = 
    sprintf "https://www.gofundme.com/mvc.php?route=homepage_norma/load_more&page=%d&term=%s&country=%s&postalCode=&locationText="
      page term country |> downloadCompressed
  HtmlDocument.Parse("<html><body>" + raw + "</body></html>")

let goFundSearch term country = 
  let rec loop p = seq {
    let doc = goFundSearchPage p term country
    let camps = doc.CssSelect(".react-campaign-tile")  
    for campaign in camps do
      let title = campaign.CssSelect(".fund-title").[0].InnerText()
      let url = campaign.CssSelect("a").[0].Attribute("href").Value()
      let location = campaign.CssSelect(".fund-location").[0].InnerText()
      yield title, url, location
    if camps.Length > 0 then yield! loop (p+1) }
  loop 1

let goFundDonations id = 
  let rec loop offset = seq {
    let funds = 
      sprintf "https://gateway.gofundme.com/web-gateway/v1/feed/%s/donations?limit=100&offset=%d&sort=recent" id offset
      |> downloadCompressed 
      |> GoFundDonations.Parse
    if funds.References.Donations.Length > 0 then
      yield! funds.References.Donations
      yield! loop (offset + 100) }
  loop 0

let goFundDetails url = 
  let doc = try downloadCompressed url |> HtmlDocument.Parse |> Some with :? System.Net.WebException -> None
  match doc with 
  | None -> None
  | Some doc ->
//  printfn "%s" url
  if doc.CssSelect(".a-created-date").Length = 0 then None else
  let created = doc.CssSelect(".a-created-date").[0].InnerText().Replace("Created ", "")
  let created = 
    if created.EndsWith " days ago" then DateTime.Today - TimeSpan.FromDays(float (created.Split(' ').[0])) 
    elif created.EndsWith " day ago" then DateTime.Today - TimeSpan.FromDays(float (created.Split(' ').[0])) 
    elif created.EndsWith " hours ago" then DateTime.Today - TimeSpan.FromHours(float (created.Split(' ').[0])) 
    elif created.EndsWith " hour ago" then DateTime.Today - TimeSpan.FromHours(float (created.Split(' ').[0])) 
    else DateTime.Parse created
  let story = doc.CssSelect(".o-campaign-story").[0].InnerText()
  let org = doc.CssSelect(".m-organization-info-content-child")
  let org = if List.length org < 2 then "", "" else org.[0].InnerText(), org.[1].InnerText()

  let prog = doc.CssSelect(".m-progress-meter-heading")
  let l1 = prog.[0].DirectInnerText()
  let l2 = prog.CssSelect("span").[0].DirectInnerText()
  let intp (s:string) = int (s.Replace("£", "").Replace(",",""))
  let raised, target = 
    if l2 = "raised" then intp l1, -1
    elif l2 = "goal" then 0, intp l1
    else intp l1, intp (l2.Replace("raised of ", "").Replace(" goal", ""))

  let id = url.Replace("https://www.gofundme.com/f/", "")
  let mrd = goFundDonations id |> Seq.tryHead |> Option.map (fun m -> m.CreatedAt)
  {| Created=created; Story=story; Organization=fst org; OrganizationDetails=snd org;
     Raised=raised; Target=target; MostRecentDonation=mrd |} |> Some

let fetchAll term =   
  let res = 
    goFundSearch term "" 
    |> Seq.filter (fun (_, _, l) -> l.Contains "United Kingdom")
  [ for t, u, l in res do
      let d = goFundDetails u
      if d.IsSome then yield {| Title=t; Url=u; Location=l; Details = d.Value |} ] 

let saveAll (all:seq<_>) file =
  let df = 
    Frame.ofRecords all
    |> Frame.expandAllCols 1

  df.ReplaceColumn("Details.MostRecentDonation",
    df.GetColumn<option<DateTimeOffset>>("Details.MostRecentDonation") |> Series.mapAll (fun _ v -> Option.flatten v))

  let df2 = df.Columns.[[  
    "Title"
    "Location"
    "Url"
    "Details.Raised"
    "Details.Target"
    "Details.MostRecentDonation"
    "Details.Created"
    "Details.Organization"
    "Details.OrganizationDetails"
    "Details.Story"
    ]]
  
  df2.SaveCsv(__SOURCE_DIRECTORY__ + "/outputs/" + file + ".csv",includeRowKeys=false)

let allFB = fetchAll "foodbank"
saveAll allFB "gofundme-foodbank"
  
let allFsB = fetchAll "food%20bank"
saveAll allFsB "gofundme-food-bank"

let allSK = fetchAll "soup%20kitchen"
saveAll allSK "gofundme-soup-kitchen"

let allH = fetchAll "homeless"
saveAll allH "gofundme-homeless"

Set.intersect
  (set [ for f in allFB -> f.Url ])
  (set [ for f in allFsB -> f.Url ])
|> Seq.length

allFB |> Seq.length
allFsB |> Seq.length
//let ch = fetchCharity 210667

  //ch.

(*
type CharityDetails = CsvProvider<const(__SOURCE_DIRECTORY__ + "/charitydetails.csv")>

let charityPage id = 
  sprintf "https://beta.charitycommission.gov.uk/charity-details/?regId=%d" id 
    |> downloadCompressed |> HtmlDocument.Parse
let charityDetails id = 
  sprintf "https://beta.charitycommission.gov.uk/umbraco/api/charityApi/ExportSearchResultsToCsv/?searchText=%d&pageNumber=1&p=1"
    id |> downloadCompressed |> CharityDetails.Parse

let chp = charityPage 210667

chp.CssSelect(".pcg-charity-details__block")
|> Seq.map (fun b -> 
  match b.CssSelect("h3") with 
  | h3::_ -> h)

let ch = charityDetails(210667).Rows |> Seq.head
ch.``Charity name``
ch.``Total spending``
ch
*)