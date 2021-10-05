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

//let today = DateTime.Parse "2020-05-17"
//let today = DateTime.Parse "2020-06-01"
//let today = DateTime.Parse "2020-06-14"
//let today = DateTime.Parse "2020-06-29"
//let today = DateTime.Parse "2020-07-12"
//let today = DateTime.Parse "2020-07-26"
//let today = DateTime.Parse "2020-08-09"
//let today = DateTime.Parse "2020-08-23"
//let today = DateTime.Parse "2020-09-06"
//let today = DateTime.Parse "2020-09-20"
//let today = DateTime.Parse "2020-10-04"
//let today = DateTime.Parse "2020-10-18"
//let today = DateTime.Parse "2020-11-01"
//let today = DateTime.Parse "2020-11-15"
//let today = DateTime.Parse "2020-11-29"
//let today = DateTime.Parse "2020-12-13"
//let today = DateTime.Parse "2020-12-27"
//let today = DateTime.Parse "2021-01-10"
//let today = DateTime.Parse "2021-01-24"
//let today = DateTime.Parse "2021-02-07"
//let today = DateTime.Parse "2021-02-21"
//let today = DateTime.Parse "2021-03-07"
//let today = DateTime.Parse "2021-03-21"
//let today = DateTime.Parse "2021-04-04"
//let today = DateTime.Parse "2021-04-18"
//let today = DateTime.Parse "2021-05-02"
//let today = DateTime.Parse "2021-05-16"
//let today = DateTime.Parse "2021-05-30"
//let today = DateTime.Parse "2021-06-13"
//let today = DateTime.Parse "2021-06-27"
//let today = DateTime.Parse "2021-07-11"
//let today = DateTime.Parse "2021-07-25"
//let today = DateTime.Parse "2021-08-08"
//let today = DateTime.Parse "2021-08-22"
//let today = DateTime.Parse "2021-09-05"
//let today = DateTime.Parse "2021-09-20"
let today = DateTime.Parse "2021-10-03"

let temp = __SOURCE_DIRECTORY__ + "/../cache/" + (today.ToString("yyyy-MM-dd"))
let outFolder = __SOURCE_DIRECTORY__ + "/../outputs/" + (today.ToString("yyyy-MM-dd"))
Directory.CreateDirectory(temp)
Directory.CreateDirectory(outFolder)

// --------------------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------------------

let sha256 (s:string) = 
  use sha = new System.Security.Cryptography.SHA256Managed()
  Convert.ToBase64String(sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(s))).Replace("/","_")

let rec downloadCompressedHash (url:string) = 
  let fn = temp + "/" + (sha256 url)
  if not (File.Exists(fn)) then 
    printfn "Downloading: %s" url
    let req = HttpWebRequest.Create(url) :?> HttpWebRequest
    req.AutomaticDecompression <- DecompressionMethods.Deflate ||| DecompressionMethods.GZip
    use resp = req.GetResponse()
    use sr = new StreamReader(resp.GetResponseStream())
    File.WriteAllText(fn, sr.ReadToEnd())
  let res = File.ReadAllText(fn)
  if res.[0] = '\000' then 
    printfn "RETRYING %s - got data with zero bytes" url
    System.Threading.Thread.Sleep(1000)
    File.Delete(fn)
    downloadCompressedHash url
  else res 

let downloadCompressed (url:string) = 
  let rec loop sleep = 
    let fn = temp + "/" + HttpUtility.UrlEncode(url)
    try
      if not (File.Exists(fn)) then 
        printfn "Downloading: %s" url
        let req = HttpWebRequest.Create(url) :?> HttpWebRequest
        req.AutomaticDecompression <- DecompressionMethods.Deflate ||| DecompressionMethods.GZip
        use resp = req.GetResponse()
        use sr = new StreamReader(resp.GetResponseStream())
        File.WriteAllText(fn, sr.ReadToEnd())
      File.ReadAllText(fn)
    with :? System.Net.WebException as e when e.Message.Contains("Too Many Requests") ->
      printfn "Too many requests. Sleeping: %d sec" sleep
      System.Threading.Thread.Sleep(sleep * 1000)
      loop (sleep * 2)
  loop 1

// --------------------------------------------------------------------------------------
// Charity search
// --------------------------------------------------------------------------------------

//type CharitySearch = WsdlService<"https://apps.charitycommission.gov.uk/Showcharity/API/SearchCharitiesV1/SearchCharitiesV1.asmx">
type FetchCharity = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/charity.json")>

//let chs = CharitySearch.GetSearchCharitiesV1Soap()

let fetchCharity id =   
  let fn = temp + "/charity-" + string id + ".json"
  if not (File.Exists(fn)) then 
    (*
    let ch = chs.GetCharityByRegisteredCharityNumber("cf92136a-f3c7-4be1-a", id)
    let js = JsonSerializer.Create()
    let sw = new StringWriter()
    js.Serialize(sw, ch)
    File.WriteAllText(fn, sw.ToString())
    *)
    None
  else
    File.ReadAllText(fn) |> FetchCharity.Parse |> Some
  
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

let toDateTime (timestamp:int64) =
  let start = DateTime(1970,1,1,0,0,0,DateTimeKind.Utc)
  start.AddSeconds(float (timestamp / 1000L)).ToLocalTime()

// --------------------------------------------------------------------------------------
// Virgin fundraisers
// --------------------------------------------------------------------------------------

let fetchVirginData kvd fname =
  let fundraisers = ResizeArray<_>()

  for f in downloadVirginFundraisers kvd 0 do 
    let p = virginFundraiserDetail f.DisplayPageUrl
    printfn "Virgin: %s Got: %A" f.DisplayPageUrl p
    match virginFundraiserDetail f.DisplayPageUrl with 
    | Some 
        ( ( ByTagAndId ("input", "fundraiserPageActivityId") pageid | ByTagAndId ("input", "fundraiserActivityId") pageid) & 
          ByTagAndId ("input", "totalDonationInputHidden") don ) ->
        let raised = don.Attribute("value").Value() |> float
        let dons = virginDonations (int (pageid.AttributeValue("value"))) |> Array.ofSeq
  
        printfn "\nFUNDRAISER: %s (%s)" f.FundraiserName f.DisplayPageUrl
        printfn "DONATIONS: %A" (Seq.length dons)
        let dates = dons |> Seq.map (fun d -> toDateTime d.DonationDatetime)
        if not (Seq.isEmpty dons) then
          //printfn "FIRST DONATION: %O" (Seq.min dates)
          //printfn "LAST DONATION: %O" (Seq.max dates)
          printfn "RAISED: GBP %f" raised

        //for n, id in Seq.zip f.CharityNames f.CharityIds do
        let n, id = Seq.zip f.CharityNames f.CharityIds |> Seq.head // not right
        let ch = fetchCharity id
        printfn "CHARITY: %s" n
        //printfn "OPERATES: %s" (ch.AreaOfOperation |> Seq.map (fun c -> c.Trim()) |> String.concat ", ")
        //printfn "POSTCODE: %s" ch.Address.Postcode
        //printfn "ACTIVITIES: %s" ch.Activities

        let vch = virginCharityDetail id
        printfn "%d" id
        let desc = 
          try
            [ for p in vch.CssSelect("#vm-create-event-homepage60-col1 p") |> Seq.tail -> p.InnerText() ]
            |> String.concat "\n"
          with _ -> 
            [ for p in vch.CssSelect("#charityHomePageDetails p") -> p.InnerText() ]
            |> String.concat "\n"

        let addr = 
          vch.CssSelect(".side-panel-no-bg p") |> Seq.tryPick (fun p ->
          if p.InnerText().Contains("Registered address") then Some(p.DirectInnerText().Trim().Replace("\n"," ").Replace("\r"," ")) 
          else None)

        //printfn "DESCRIPTION: %s" (desc.Replace("\r", " ").Replace("\n", " "))
        //printfn "ADDRESS: %s" addr
        
        {|Link = "https://uk.virginmoneygiving.com/fundraiser-display/" + f.DisplayPageUrl
          //Fundraiser = f.FundraiserName
          Raised = int raised
          Charity = n
          Created (* First donation.. *) = if Seq.isEmpty dates then "" else (Seq.min dates).ToString("yyyy-MM-dd")
          MostRecentDonation = if Seq.isEmpty dates then "" else (Seq.max dates).ToString("yyyy-MM-dd")
          Operates = match ch with Some ch -> ch.AreaOfOperation |> Seq.map (fun c -> c.Trim()) |> String.concat ", " | _ -> ""
          Postcode = match ch with Some ch -> ch.Address.Postcode | _ -> ""
          Activities = match ch with Some ch -> ch.Activities | _ -> ""
          Description = desc.Replace("\r", " ").Replace("\n", " ")
          Complete = int raised = (dons |> Seq.choose (fun d -> try Some(int d.GrossAmount) with _ -> None) |> Seq.sum)
          Donations = dons |> Array.choose (fun d -> try Some (sprintf "%s %d" ((toDateTime d.DonationDatetime).ToString("yyyy-MM-dd")) d.GrossAmount) with _ -> None)  |> String.concat "/"
          Address = defaultArg addr "" |} |> fundraisers.Add
    | _ -> ()

  let df = Frame.ofRecords fundraisers
  df.SaveCsv(outFolder + "/" + fname)

let doitVirgin () = 
  fetchVirginData "food bank" "virgin_food-bank.csv"
  fetchVirginData "soup kitchen" "virgin_soup-kitchen.csv"
  fetchVirginData "foodbank" "virgin_foodbank.csv"
  fetchVirginData "homeless" "virgin_homeless.csv"


// --------------------------------------------------------------------------------------
// GoFundMe
// --------------------------------------------------------------------------------------

type GoFundDonations = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/gofund-donations.json")>

let goFundSearchPage page term country = 
  let url = 
    sprintf "https://www.gofundme.com/mvc.php?route=homepage_norma/load_more&page=%d&term=%s&country=%s&postalCode=&locationText="
      page term country 
  let raw = url |> downloadCompressed
  HtmlDocument.Parse("<html><body>" + raw + "</body></html>")

let goFundSearch term country = 
  let rec loop p = seq {
    let doc = goFundSearchPage p term country
    let camps = doc.CssSelect(".fund_tile_card_link")  
    for campaign in camps do
      let title = campaign.CssSelect(".fund-title").[0].InnerText()
      let url = campaign.CssSelect("a").[0].Attribute("href").Value()
      let location = campaign.CssSelect(".fund-location").[0].InnerText()
      yield title, url, location
(*
    let camps = doc.CssSelect(".react-campaign-tile")  
    for campaign in camps do
      let title = campaign.CssSelect(".fund-title").[0].InnerText()
      let url = campaign.CssSelect("a").[0].Attribute("href").Value()
      let location = campaign.CssSelect(".fund-location").[0].InnerText()
      yield title, url, location
      *)
    if camps.Length > 0 then yield! loop (p+1) }
  loop 1

let goFundDonations id = 
  let rec loop offset = seq {
    let funds = 
      try
        sprintf "https://gateway.gofundme.com/web-gateway/v1/feed/%s/donations?limit=100&offset=%d&sort=recent" id offset
        |> downloadCompressed 
        |> GoFundDonations.Parse |> Some
      with _ -> None
    if funds.IsSome && funds.Value.References.Donations.Length > 0 then
      yield! funds.Value.References.Donations
      yield! loop (offset + 100) }
  loop 0

let goFundDetails (title, location) url = 
  let doc = try downloadCompressed url |> HtmlDocument.Parse |> Some with :? System.Net.WebException as e -> printfn "Failed: %A" e; None
  match doc with 
  | None -> None
  | Some doc ->
  if doc.CssSelect(".a-created-date").Length = 0 then None else
  let created = doc.CssSelect(".a-created-date").[0].InnerText().Replace("Created ", "")
  let created = 
    if created.EndsWith " days ago" then today - TimeSpan.FromDays(float (created.Split(' ').[0])) 
    elif created.EndsWith " day ago" then today - TimeSpan.FromDays(float (created.Split(' ').[0])) 
    elif created.EndsWith " hours ago" then today - TimeSpan.FromHours(float (created.Split(' ').[0])) 
    elif created.EndsWith " hour ago" then today - TimeSpan.FromHours(float (created.Split(' ').[0])) 
    elif created = "Just now" then today
    else try DateTime.Parse created with e -> failwithf "Wrong date: %s" created 
  let story = match doc.CssSelect(".o-campaign-story") with s::_ -> s.InnerText() | _ -> ""
  let org = doc.CssSelect(".m-organization-info-content-child")
  let org = if List.length org < 2 then "", "" else org.[0].InnerText(), org.[1].InnerText()

  let prog = doc.CssSelect(".m-progress-meter-heading")
  let l1, l2 = 
    if prog = [] then 
      let prog = doc.CssSelect(".m-progress-meter-heading--exp")
      prog.[0].CssSelect("div").[0].DirectInnerText(),
      prog.[0].CssSelect("div span").[0].DirectInnerText()
    else
      prog.CssSelect("span").[0].DirectInnerText(),
      prog.[0].DirectInnerText()

  if l1.Contains "€" || l2.Contains "€" || l1.Contains "$" || l2.Contains "$" then None else // Ignore dollars (and euros...)
  let intp (s:string) = int (s.Replace("£", "").Replace(",",""))
  let raised, target = 
    try 
      if l2.Trim() = "raised" then intp l1, -1
      elif l1.Trim() = "raised" then intp l2, -1
      elif l2.Trim() = "goal" then 0, intp l1
      elif l1.Trim() = "goal" then 0, intp l2
      elif l1.Contains("raised of") && l1.Contains("goal") then 
        intp (l1.Replace("raised of ", "").Replace(" goal", "")), intp l2
      else 
        intp l1, intp (l2.Replace("raised of ", "").Replace(" goal", "").Replace(" target", ""))
    with e ->
      failwithf "Faied to parse raised: %s | %s" l1 l2

  let id = url.Replace("https://www.gofundme.com/f/", "").Replace("https://uk.gofundme.com/f/", "")
  let dons, mrd, doncnt, donsum = 
    try
      let dons = goFundDonations id |> Array.ofSeq
      dons |> Seq.map (fun d -> sprintf "%s %d" (d.CreatedAt.ToString("yyyy-MM-dd")) d.Amount) |> String.concat "/",
      dons |> Seq.tryHead |> Option.map (fun m -> m.CreatedAt.ToString("yyyy-MM-dd")) |> Option.defaultValue "",
      dons |> Seq.length,
      dons |> Seq.sumBy (fun d -> d.Amount)
    with :? System.Net.WebException as we when we.Message.Contains("404") -> "", "", -1, -1
  {| Title = title
     Location = location
     Link = url
     Complete = int raised = donsum
     Created = created.ToString("yyyy-MM-dd")
     Story = story.Replace('\n', ' ').Replace('\r', ' ').Replace(',', ' ')
     Organization = fst org
     OrganizationDetails = snd org
     Raised = raised
     Target = target
     Donations = dons
     MostRecentDonation = mrd
     DonationCount = doncnt |} |> Some

let fetchAll term =   
  let res = 
    goFundSearch term "" 
    |> Seq.filter (fun (_, _, l) -> l.Contains "United Kingdom") |> Array.ofSeq
  [ for i, (t, u, l) in Seq.indexed res do
      printfn "SCRAPING (%d/%d): %s" i res.Length u
      yield! goFundDetails (t, l) u |> Option.toList ] 

let saveAll (all:seq<_>) file =
  let df = 
    Frame.ofRecords all
    |> Frame.expandAllCols 1

  let df2 = df.Columns.[[  
    "Title"
    "Location"
    "Link"
    "Raised"
    "Target"
    "MostRecentDonation"
    "Created"
    "Organization"
    "OrganizationDetails"
    "DonationCount"
    "Donations"
    "Story"
    "Complete"
    ]]
  
  df2.SaveCsv(outFolder + "/" + file + ".csv",includeRowKeys=false)

let doitGoFundMe () = 
  let allFB = fetchAll "foodbank"
  saveAll allFB "gofundme_foodbank"
  
  let allFsB = fetchAll "food%20bank"
  saveAll allFsB "gofundme_food-bank"

  let allSK = fetchAll "soup%20kitchen"
  saveAll allSK "gofundme_soup-kitchen"

  let allH = fetchAll "homeless"
  saveAll allH "gofundme_homeless"

  Set.intersect
    (set [ for f in allFB -> f.Link ])
    (set [ for f in allFsB -> f.Link ])
  |> Seq.length
  |> ignore

  allFB |> Seq.length
  |> ignore
  
  allFsB |> Seq.length
  |> ignore

// --------------------------------------------------------------------------------------
// Just Giving
// --------------------------------------------------------------------------------------

let q1 = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%228c392d676485536b0683c034bc6a1bfe1c7ec8be8c543d6c605dd5d07aef3aef%22%7D%7D"
let q2 = "https://graphql.justgiving.com/?operationName=SupportersList&variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22cb07ff9d6a37d17365e726d4aa9a930da3608338824678804ef81aa496dced4e%22%7D%7D"
let q3 = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%229c2597a08b07fc15681a01ebd6c97af6519d2ca72b6c974cd1b980e546668cef%22%7D%7D"
let q4 = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2278a2eb195bec32b8011d5e6a7fed079635a1a1049ea9b0e60026f6b33dd8e9a4%22%7D%7D"
let q5 = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22b1af160411150e1fa5e5c5beea535f3739d1a4b6aa9d959d9d0c87ee1dfe943c%22%7D%7D"
let q6 = "https://graphql.justgiving.com/?operationName=QualarooLoaderDocument&variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22d5f83efd402a38ecce069d18ad10546d61ee3d12f707db4dd8ea68fbf34fd69d%22%7D%7D"
let q7 = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%226bc1cf401475d81b1eb048bf75c0fe9d6484babc53988c0188aee9b97accaae6%22%7D%7D"
let q8 = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%221ca2a5e29e0df3563d9aef1fc000d7f9e90086457e69113781750209850b5c4a%22%7D%7D"
let q9 = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22d68eb42b36143913d23ada1d1d434c19d160ebada94ebc3cdcc719e8a2647f34%22%7D%7D"
let qA = "https://graphql.justgiving.com/?operationName=AboutCampaignAndCharity&variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%2C%22withExternalUrl%22%3Afalse%2C%22beneficiariesLimit%22%3A6%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%226dcfcd65d1a7bc33cf2a7cde7c3d997b1afc9dd7fec02222894fdf0f2d83a4ed%22%7D%7D"
let qB = "https://graphql.justgiving.com/?operationName=GetPageCampaignIds&variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2243df7a2996bd2ba628aacdd8974d91418bd23d77cb8bea04a39765489f7db1db%22%7D%7D"
let qC = "https://graphql.justgiving.com/?operationName=ListTimelineEntries&variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%2C%22measurementSystem%22%3A%22METRIC%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%221b3e63a054b9a47be80b9cdfae75f240d93443d79163ee35c2fc4a95dfd7e969%22%7D%7D"
let qD = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%220a24ab8af72dd7ccaf8a5922e4ee5714635dba2aa796bb0defeedbc031693052%22%7D%7D"
let qE = "https://graphql.justgiving.com/?variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22fulwood-foodbank-appeal%22%2C%22preview%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%227e811938a44d4a74ca07ea107c5b0a8e9ca32b28b5ceeab798bb781520626707%22%7D%7D"
let qS = "https://graphql.justgiving.com/?operationName=SupportersList&variables=%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22finleyandnanuk%22%2C%22preview%22%3Afalse%2C%22after%22%3A%22MTA%3D%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22cb07ff9d6a37d17365e726d4aa9a930da3608338824678804ef81aa496dced4e%22%7D%7D"

let saveSamples() = 
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q1.json", downloadCompressedHash q1)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q2.json", downloadCompressedHash q2)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q3.json", downloadCompressedHash q3)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q4.json", downloadCompressedHash q4)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q5.json", downloadCompressedHash q5)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q6.json", downloadCompressedHash q6)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q7.json", downloadCompressedHash q7)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q8.json", downloadCompressedHash q8)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-q9.json", downloadCompressedHash q9)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-qA.json", downloadCompressedHash qA)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-qB.json", downloadCompressedHash qB)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-qC.json", downloadCompressedHash qC)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-qD.json", downloadCompressedHash qD)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-qE.json", downloadCompressedHash qE)
  IO.File.WriteAllText(__SOURCE_DIRECTORY__ + "/samples/just-details-qS.json", downloadCompressedHash qS)


type JustSearch = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-search.json")>
type JustDetails1 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q1.json")>
type JustDetails2 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q2.json")>
type JustDetails3 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q3.json")>
type JustDetails4 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q4.json")>
type JustDetails5 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q5.json")>
type JustDetails6 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q6.json")>
type JustDetails7 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q7.json")>
type JustDetails8 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q8.json")>
type JustDetails9 = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-q9.json")>
type JustDetailsA = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-qA.json")>
type JustDetailsB = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-qB.json")>
type JustDetailsC = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-qC.json")>
type JustDetailsD = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-qD.json")>
type JustDetailsE = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-qE.json")>
type JustSupporters = JsonProvider<const(__SOURCE_DIRECTORY__ + "/samples/just-details-qS.json")>

let fetchSupporters id = 
  let rec loop curs = seq {
    let vars = sprintf """{"type":"FUNDRAISING","slug":"%s","preview":false,"after":"%s"}""" id curs
    let url = qS.Replace("%7B%22type%22%3A%22FUNDRAISING%22%2C%22slug%22%3A%22finleyandnanuk%22%2C%22preview%22%3Afalse%2C%22after%22%3A%22MTA%3D%22%7D", vars)
    let sup = downloadCompressedHash url |> JustSupporters.Parse
    let dons = try Some sup.Data.Page.Donations with _ -> None
    if dons.IsSome then
      for d in dons.Value.Edges do yield d.Node
      if dons.Value.PageInfo.HasNextPage then
        yield! loop dons.Value.PageInfo.EndCursor }
  loop ""

let fetchJustData kvd fname = 
  let srch = 
    "https://www.justgiving.com/onesearch/query?limit=10000&q=" + kvd + "&i=fundraiser&offset=0"
    |> downloadCompressed 
    |> JustSearch.Parse

  let uk = 
    srch.GroupedResults.[0].Results |> Seq.filter (fun f -> f.CountryCode = "United Kingdom")

  let all = 
    [ for i, f in Seq.indexed uk do
       printfn "%d/%d (%s)" i (Seq.length uk) f.Link
       let d1s = downloadCompressedHash (q1.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1)))
       //printfn "%s" (d1s.Substring(0, 10))
       if f.Link <> "https://www.justgiving.com/Portraitsbyhercule" then
        let d1 = JustDetails1.Parse(downloadCompressedHash (q1.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        let d2 = JustDetails2.Parse(downloadCompressedHash (q2.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let d3 = JustDetails3.Parse(downloadCompressedHash (q3.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let d4 = JustDetails4.Parse(downloadCompressedHash (q4.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let d5 = JustDetails5.Parse(downloadCompressedHash (q5.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        let d6 = JustDetails6.Parse(downloadCompressedHash (q6.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let d7 = JustDetails7.Parse(downloadCompressedHash (q7.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let d8 = JustDetails8.Parse(downloadCompressedHash (q8.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let d9 = JustDetails9.Parse(downloadCompressedHash (q9.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let dA = JustDetailsA.Parse(downloadCompressedHash (qA.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let dB = JustDetailsB.Parse(downloadCompressedHash (qB.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let dC = JustDetailsC.Parse(downloadCompressedHash (qC.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let dD = JustDetailsD.Parse(downloadCompressedHash (qD.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //let dE = JustDetailsE.Parse(downloadCompressedHash (qE.Replace("fulwood-foodbank-appeal", f.LinkPath.Substring(1))))
        //printfn "\nSCRAPING: %s (%s)" f.Name f.Link
        let ch = try fetchCharity f.CharityId with _ -> None

        if (try ignore(d1.Data.Page.Relationships); true with _ -> false) then
          let sups = fetchSupporters (f.LinkPath.Substring(1)) |> Array.ofSeq
          let raised = d6.Data.Page.DonationSummary.TotalAmount.Value / 100
          yield 
           {| Title = f.Name
              Link = f.Link
              CharityName = d1.Data.Page.Relationships.Beneficiaries.Nodes.[0].Name
              CharityId = d1.Data.Page.Relationships.Beneficiaries.Nodes.[0].RegistrationNumber
              CharityAddress = try [ for k, v in ch.Value.Address.JsonValue.Properties() do if not (String.IsNullOrWhiteSpace(v.AsString())) then yield v.AsString().Trim() ] |> String.concat ", " with _ -> ""
              CharityAreaOfOperation = try ch.Value.AreaOfOperation |> Seq.map (fun c -> c.Trim()) |> String.concat ", " with _ -> ""
              CharityAreaOfBenefit = try ch.Value.AreaOfBenefit with _ -> ""
              MostRecentDonation = try d2.Data.Page.Donations.Edges |> Seq.tryHead |> Option.map (fun d -> d.Node.CreationDate.ToString("yyyy-MM-dd")) |> Option.defaultValue "" with _ -> ""
              Raised = raised
              Target = 
                try d6.Data.Page.TargetWithCurrency.Value / 100 with e ->
                  let r = d6.Data.Page.TargetWithCurrency.JsonValue.["value"].AsDecimal()
                  int (r / 100M)
              //Owner = d5.Data.Page.Owner.Name 
              Created = d6.Data.Page.CreateDate.ToString("yyyy-MM-dd")
              DonationDetailCount = sups |> Seq.length
              DonationCount = 
                if raised = 0 then 0 
                elif d6.Data.Page.DonationSummary.JsonValue.["donationCount"] = JsonValue.Null then 0 
                else try d6.Data.Page.DonationSummary.DonationCount with _ -> failwithf "%A" d6.Data.Page.DonationSummary
              Complete = int raised = (sups |> Seq.choose (fun d -> try Some(int d.Amount.Value / 100) with _ -> None) |> Seq.sum)
              Donations = 
                sups |> Array.choose (fun d -> try Some(sprintf "%s %d" (d.CreationDate.ToString("yyyy-MM-dd")) (d.Amount.Value / 100)) with _ -> None)
                  |> String.concat "/"
              |} ]
  //  all |> Seq.length
  //  all |> Seq.countBy (fun ch -> ch.CharityName) |> Seq.sortByDescending snd |> Seq.iter (printfn "%A")
  //  all |> Seq.countBy (fun ch -> ch.CharityName) |> Seq.length
  let df3 = Frame.ofRecords all
  df3.SaveCsv(outFolder + "/" + fname + ".csv",includeRowKeys=false)

let doitJust () = 
  fetchJustData "foodbank" "just_foodbank"
  fetchJustData "food%20bank" "just_food-bank"
  fetchJustData "soup%20kitchen" "just_soup-kitchen"
  fetchJustData "homeless" "just_homeless"

let mutable finished = false
while not finished do
  try
    //doitVirgin ()
    //doitGoFundMe ()
    doitJust ()
    finished <- true
  with _ ->
    ()
