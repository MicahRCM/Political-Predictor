-- flair,
explain select 
    author,
    subreddit,
    flair,
    count(*) as comments,
    sum(score)    
from comment 
where author in (
    select distinct(author)
    from comment
    where subreddit = 'DotA2'
)
group by author,subreddit,flair ;

CREATE INDEX idx_subreddit_comment
on comment(subreddit);

select count(subreddit) from comment;


CREATE INDEX idx_author_subreddit_comment_score
on comment(author,subreddit,flair,score);

VACUUM Analyze comment;

------------------------------ COMMANDS -------------
1. create index 
CREATE INDEX idx_author_subreddit_flair_prediction
on comment(author,subreddit,flair,score);

2. create table form query
create table pcm_flair_prediction_1 as (select 
    author,
    subreddit,
    flair,
    count(*) as posts,
    sum(score)    
from comment 
where author in (
    select distinct(author)
    from comment
    where subreddit = 'PoliticalCompassMemes'
    and created_utc > 1561153400
)
group by author,subreddit,flair) ;

CREATE INDEX idx_ppm_flair_prediction_author
on ppm_flair_prediction(author);

CREATE INDEX idx_ppm_flair_prediction_subreddit
on ppm_flair_prediction(subreddit);

3. Dont list people that have changed flair
-> subreddit dipersion is 50 users
select * 
from pcm_flair_prediction_1
where author in (
    select author
    from 
    (select author,flair, count(*)
        from pcm_flair_prediction_1
        where flair is not null
        and flair != 0
        group by author, flair) as t1
    group by author
    having count(*) = 1
) and subreddit in (
    select subreddit
    from pcm_flair_prediction_1
    group by subreddit
    having count(*) >= 50
);


-------------- test

create table askreddit_flair_prediction as (select 
        author,
        subreddit,
        flair,
        count(*) as posts,
        sum(score)    
    from comment 
    where author in (
        select distinct(author)
        from comment
        where subreddit = 'AskReddit'
    )
    group by author,subreddit,flair
);


select * 
from askreddit_flair_prediction  
where author in (
    select author
    from 
    (select author,flair, count(*)
        from askreddit_flair_prediction
        group by author, flair) as t1
    group by author
    having count(*) = 1
) and subreddit in (
    select subreddit,count(*)
    from askreddit_flair_prediction
    group by subreddit
    having count(*) > 50
);


select *
from pcm_flair_prediction_1
where author in (
    select author
    from 
    (select author,flair, count(*)
        from pcm_flair_prediction_1
        where flair is not null
        and flair != 0
        group by author, flair) as t1
    group by author
    having count(*) = 1
) and subreddit in (
    select subreddit
    from pcm_flair_prediction_1
    group by subreddit
    having count(*) >= 50
);

ALTER TABLE comment ADD redditint bigint;
UPDATE comment set redditint = base36_decode(reddit_id); 

ALTER TABLE posts ADD redditint bigint;
UPDATE posts set redditint = base36_decode(pid);

ALTER TABLE pcm_user_posts ADD flair_corrected integer;
ALTER TABLE pcm_user_posts_corrected ADD avg double precision;
UPDATE pcm_user_posts_corrected set avg = points::decimal/n;
UPDATE pcm_user_posts set flair_corrected = flair;

1 = 1,3,7
2 = 2,8,9
3 = 4,5,6

update pcm_user_posts set flair_corrected = 1 where flair_corrected = ANY(ARRAY[1,3,7]);
update pcm_user_posts set flair_corrected = 2 where flair_corrected = ANY(ARRAY[2,8,9]);
update pcm_user_posts set flair_corrected = 3 where flair_corrected = ANY(ARRAY[4,5,6]);

ALTER TABLE pcm_user_comments ADD flair_corrected integer;
UPDATE pcm_user_comments set flair_corrected = flair;
update pcm_user_comments set flair_corrected = 1 where flair_corrected = ANY(ARRAY[1,3,7]);
update pcm_user_comments set flair_corrected = 2 where flair_corrected = ANY(ARRAY[2,8,9]);
update pcm_user_comments set flair_corrected = 3 where flair_corrected = ANY(ARRAY[4,5,6]);

ALTER TABLE pcm_user_comments_corrected RENAME COLUMN n TO points;


select author
from (
    select author
    from pcm_dream_union
    where flair_corrected != 0
        and subreddit = 'PoliticalCompassMemes'
    group by author
    having count(*) = 1
) as t1
group by author
having count(*) = 1


(select author,subreddit,flair_corrected
from pcm_user_comments as pcm_uc limit 10)
UNION
(select author,subreddit,flair_corrected
from pcm_user_posts as pcm_up limit 10)


select pcm_uc.author as pcm_c_author,
    pcm_up.author as pcm_p_author,
    pcm_uc.subreddit as pcm_c_subreddit,
    pcm_up.subreddit as pcm_p_subreddit,
    pcm_uc.flair_corrected as pcm_c_flair_corrected,
    pcm_up.flair_corrected as pcm_p_flair_corrected
from  pcm_user_posts_corrected as pcm_up
    FULL OUTER JOIN pcm_user_comments_corrected as pcm_uc
    ON pcm_uc.author = pcm_up.author
    AND pcm_uc.subreddit = pcm_up.subreddit
    AND pcm_uc.flair_corrected = pcm_up.flair_corrected;


select pcm_uc.author as pcm_c_author,
    pcm_up.author as pcm_p_author,
    pcm_uc.subreddit as pcm_c_subreddit,
    pcm_up.subreddit as pcm_p_subreddit,
    pcm_uc.flair_corrected as pcm_c_flair_corrected,
    pcm_up.flair_corrected as pcm_p_flair_corrected,
    pcm_uc.avg as pcm_c_avg,
    pcm_up.avg as pcm_p_avg,
    pcm_uc.n as pcm_c_n,
    pcm_up.n as pcm_p_n
from  pcm_user_posts_corrected as pcm_up
    FULL OUTER JOIN pcm_user_comments_corrected as pcm_uc
    ON pcm_uc.author = pcm_up.author
    AND pcm_uc.subreddit = pcm_up.subreddit
    AND pcm_uc.flair_corrected = pcm_up.flair_corrected;  


create table pcm_user_comments_corrected as 
    (select 
        author,
        subreddit,
        flair_corrected,
        sum(n) as n,
        sum(points) as points    
    from pcm_user_comments
    group by author,subreddit,flair_corrected);
ALTER TABLE pcm_user_comments_corrected ADD avg double precision;
UPDATE pcm_user_comments_corrected set avg = points::decimal/n;

update comment set flair = 0 where flair is null and su;


create table pcm_dream_union as (
    select pcm_uc.author as pcm_c_author,
    pcm_up.author as pcm_p_author,
    pcm_uc.subreddit as pcm_c_subreddit,
    pcm_up.subreddit as pcm_p_subreddit,
    pcm_uc.flair_corrected as pcm_c_flair_corrected,
    pcm_up.flair_corrected as pcm_p_flair_corrected,
    pcm_uc.avg as pcm_c_avg,
    pcm_up.avg as pcm_p_avg,
    pcm_uc.n as pcm_c_n,
    pcm_up.n as pcm_p_n
from  pcm_user_posts_corrected as pcm_up
    FULL OUTER JOIN pcm_user_comments_corrected as pcm_uc
    ON pcm_uc.author = pcm_up.author
    AND pcm_uc.subreddit = pcm_up.subreddit
    AND pcm_uc.flair_corrected = pcm_up.flair_corrected
);

drop table pcm_dream_union;


ALTER TABLE pcm_dream_union ADD author text;
ALTER TABLE pcm_dream_union ADD subreddit text;



ALTER TABLE pcm_dream_union 
DROP COLUMN pcm_c_author;

ALTER TABLE pcm_dream_union 
DROP COLUMN pcm_p_author;

select author,count(*)
from pcm_dream_union
where flair_corrected != 0
    and subreddit = 'PoliticalCompassMemes'
group by author having count(*) > 1;

select author
from pcm_dream_union
where flair_corrected != 0
    and subreddit = 'PoliticalCompassMemes'
group by author having count(*) = 1

select *
from pcm_dream_union
where author in (
    select author
    from pcm_dream_union
    where flair_corrected != 0
        and subreddit = 'PoliticalCompassMemes'
    group by author having count(*) = 1
) and subreddit in (
    select subreddit
    from pcm_user_comments_corrected
    where author in (
        select author
        from pcm_dream_union
        where flair_corrected != 0
            and subreddit = 'PoliticalCompassMemes'
        group by author having count(*) = 1
    )
    group by subreddit
    having count(*) >= 75
)  order by subreddit, pcm_p_n ;


select subreddit
from pcm_user_comments_corrected
where author in (
    select author
    from pcm_dream_union
    where flair_corrected != 0
        and subreddit = 'PoliticalCompassMemes'
    group by author having count(*) = 1
)
group by subreddit
having count(*) >= 10


-- subreddit
-- 600 subreddits
-- 


-- 2nd table
-- comments |< 600 subreddit
-- post |< 600 subreddit


create table predictions_values (
    author varchar,
    lib_c_n integer,
);

-- no of comments
select count(*) from comment where subreddit = ANY(ARRAY[]);


create table predictions_comment_set as (
    select author, subreddit, count(*) as n, sum(score) as points
    from comment
    where created_utc > 1561153400 and
    subreddit = ANY(ARRAY['2meirl4meirl','aaaaaaacccccccce','ABoringDystopia','accidentallycommunist','AccidentalWesAnderson','actuallesbians','ADHD','AgainstHateSubreddits','agedlikemilk','ainbow','Anarchism','anarchocommunism','Anarchy101','AnCapMemes','AngrySocialist','ANI_COMMUNISM','AnimalsBeingBros','AnticommieCringe','AntifaHedgewik','AntifascistsofReddit','antifastonetoss','antiwork','AOC','AsABlackMan','asexuality','askphilosophy','asktransgender','assholedesign','AteTheOnion','atheism','Badfaketexts','BadMensAnatomy','badphilosophy','badwomensanatomy','BannedFromThe_Donald','beholdthemasterrace','bernieblindness','bestof','BestOfOutrageCulture','bi_irl','billwurtzmemes','bisexual','BlackPeopleTwitter','blessedimages','blunderyears','BoneAppleTea','bonehurtingjuice','boomershumor','BoomerTears','boottoobig','BrandNewSentence','bread_irl','BreadStapledToTrees','BreadTube','Breath_of_the_Wild','britishproblems','brooklynninenine','canadaleft','capitalism_in_decay','CasualUK','chaoticgood','ChapoTrapHouse','chapotraphouse2','chapotraphouse2_2_2','chomsky','circlebroke2','ClimateActionPlan','ClimateMemes','ClimateOffensive','comedyhomicide','comics','communism','communism101','CommunismMemes','COMPLETEANARCHY','conservativecartoons','ContraPoints','CorporateFacepalm','creepyPMs','CrewsCrew','Cultural_Marxism_irl','curlyhair','Cyberpunk','DankLeft','daverubin','DemocraticSocialism','depression','Destiny','discordapp','disneyvacation','dsa','EarthStrike','educationalgifs','egg_irl','ElizabethWarren','ENLIGHTENEDCENTRISM','ennnnnnnnnnnnbbbbbby','EnoughMuskSpam','enoughpetersonspam','EnoughTrumpSpam','entertainment','esist','ExtinctionRebellion','Eyebleach','fantanoforever','FanTheories','feemagers','Feminism','fireemblem','forwardsfromgrandma','forwardsfromhitler','ForwardsFromKlandma','FragileMaleRedditor','FragileWhiteRedditor','FrankOcean','freefolk','ftm','Fuckthealtright','FULLCOMMUNISM','gamegrumps','gameofthrones','GamerGhazi','Gamingcirclejerk','gatekeeping','gatesopencomeonin','gay','gay_irl','gaymers','GaySoundsShitposts','GenZanarchist','GenZedong','GetMotivated','GirlGamers','GoodFakeTexts','GreenAndPleasant','HailCorporate','happycowgifs','Hasan_Piker','hbomberguy','HelicopterAddict','hiphopheads','HumansBeingBros','iamverysmart','ifyoulikeblank','ihavesex','IncelTears','indieheads','InfowarriorRides','insaneparents','insanepeoplefacebook','InsanePeopleQuora','inspirobot','IronFrontUSA','IsTodayFridayThe13th','JacksFilms','JustLearnedTheFWord','Labour','LabourUK','LandlordLove','LateStageCapitalism','LateStageFeudalism','LateStageGenderBinary','LateStageImperialism','LeftieZ','LeftistGamersUnion','leftistvexillology','LeftWithoutEdge','lewronggeneration','lgbt','LGBTeens','LGBTnews','likeus','linkiscute','listentothis','lostgeneration','MadeMeSmile','magicthecirclejerking','MaliciousCompliance','mapporncirclejerk','MarchAgainstNazis','masterforgiveme','me_irl','me_irlgbt','meirl','mendrawingwomen','MensLib','menwritingwomen','MEOW_IRL','MoreTankieChapo','MovieDetails','movies','moviescirclejerk','MtF','Music','MutualSupport','mythologymemes','NetflixBestOf','niceguys','NobodyAsked','NonBinary','notdisneyvacation','nothingeverhappens','NotHowGirlsWork','NotKenM','notliketheothergirls','okboomerretard','OldSchoolCool','onejoke','onguardforthee','OurPresident','pansexual','PhilosophyTube','pointlesslygendered','Political_Revolution','politics','PragerUrine','PresidentialRaceMemes','PropagandaPosters','ProRevenge','punk','QueerVexillology','Qult_Headquarters','RadicalChristianity','rareinsults','reactiongifs','RedditInReddit','RedsKilledTrillions','religiousfruitcake','rickandmorty','RightCringe','rimjob_steve','rojava','RTGameCrowd','SampleSize','SandersForPresident','SapphoAndHerFriend','sbubby','ScottishPeopleTwitter','seculartalk','SelfAwarewolves','SequelMemes','ShitAmericansSay','shitfascistssay','ShitLiberalsSay','ShitMomGroupsSay','ShitRedditSays','shittymoviedetails','shittyrobots','Sigmarxism','Sino','sjws_bad','SmugIdeologyMan','socialism','Socialism_101','SocialistGaming','SocialistRA','SRAWeekend','StarWars','Stonetossingjuice','stopperpacks','StupidFood','SubredditSimMeta','SubSimulatorGPT2Meta','SubwayCreatures','suddenlybi','SuddenlyCommunist','SuddenlyGay','SuddenlyTrans','surrealmemes','suspiciouslyspecific','swoletariat','SyndiesUnited','TargetedShirts','TechNewsToday','technology','terriblefacebookmemes','terriblefandommemes','tf_irl','thanksimcured','ThatsHowThingsWork','The_Leftorium','THE_PACK','TheBluePill','TheOnion','therewasanattempt','TheRightCantMeme','ThisButUnironically','tipofmytongue','ToiletPaperUSA','TopMindsOfReddit','traaaaaaannnnnnnnnns','traaNSFW','trans','transgender','transgendercirclejerk','transpositive','transtimelines','TransyTalk','TrollCoping','TrollXChromosomes','TrumpCriticizesTrump','tumblr','TwoXChromosomes','u_userleansbot','UNBGBBIIVCHIDCTIICBG','UnethicalLifeProTips','unitedkingdom','VaushV','vaxxhappened','vegan','vegancirclejerk','vegetarian','VeryGay','vexillologycirclejerk','VoteBlue','WayOfTheBern','WhereAreTheChildren','wholesomebpt','wholesomememes','wholesomeyuri','WitchesVsPatriarchy','wlw_irl','worldnews','wowthanksimcured','YUROP','ZeroWaste','AnimalsBeingJerks','ANormalDayInRussia','Art','AskAnAmerican','austriahungary','badeconomics','Battlefield','BeAmazed','blackpeoplegifs','carporn','CasualConversation','centerleftpolitics','centrist','collapse','comedyamputation','CozyPlaces','Cringetopia','CryptoCurrency','Damnthatsinteresting','DankMemesFromSite19','dashcamgifs','dataisbeautiful','de','DeathtoAmeriKKKa','DebateAltRight','dirtbagcenter','DnD','dndmemes','dogelore','dogswithjobs','Drama','Enough_Sanders_Spam','EnoughCommieSpam','FellowKids','food','Futurology','gadgets','GamePhysics','GamersRiseUp','gaybros','geopolitics','gunpolitics','Hiphopcirclejerk','IAmA','imsorryjon','instant_regret','interestingasfuck','JustUnsubbed','Kaiserposting','Kaiserreich','KidsAreFuckingStupid','knives','KRCJ','LatinoPeopleTwitter','linguisticshumor','MakeupAddiction','mealtimevideos','megalophobia','MilitaryGfys','MilitaryPorn','Morrowind','MostBeautiful','NatureIsFuckingLit','neoconNWO','neoliberal','NewPatriotism','NoahGetTheBoat','nottheonion','okbuddyredacted','okmetaretard','OutOfTheLoop','paradoxplaza','pcgaming','perfectlycutscreams','Pete_Buttigieg','philosophy','quityourbullshit','rpghorrorstories','sadcringe','science','SCP','scriptedasiangifs','ShitPostCrusaders','shittyfoodporn','ShitWehraboosSay','space','SpeedOfLobsters','sports','starterpacks','subredditcancer','SubredditDrama','television','ThesaurizeThis','theyknew','ThingsCutInHalfPorn','thisismylifenow','tifu','TIHI','timeshiftedmemes','todayilearned','toptalent','trippinthroughtime','TumblrInAction','ukpolitics','videos','wallstreetbets','Watches','woahdude','woof_irl','WOSH','YangGang','youtubehaiku','4chan','AgainstDegenerateSubs','Anarcho_Capitalism','ape','Apustaja','ar15','askaconservative','AskLibertarians','AskThe_Donald','average_redditor','averageredditor','banned','BenShapiroMemes','Boomer','brasilivre','Capitalism','Catholicism','CatholicMemes','circlejerk','classic4chan','Conservative','conservatives','ConsumeProduct','CringeAnarchy','CrusadeMemes','CursedGuns','dankmemes','DarkHumorAndMemes','DarkJokeCentral','DeclineIntoCensorship','drumpfisfinished','edgydarkdankmemes','Firearms','FreeSpeech','frenworld','GenZ','GoldandBlack','Government_is_lame','GunPorn','guns','ImGoingToHellForThis','itsafetish','Jordan_Peterson_Memes','KotakuInAction','Libertarian','libertarianmeme','LouderWithCrowder','Megumin','MensRights','metacanada','MGTOW','Military','monarchism','MURICA','nattyorjuice','neogaianism','NoFap','NOWTTYG','pcmasterrace','PewdiepieSubmissions','Political_Tumor','PoliticalCompassMemes','progun','prolife','pussypassdenied','QualitySocialism','reclassified','Republican','RightwingLGBT','SargonofAkkad','shitguncontrollerssay','ShitNeoconsSay','ShitPoliticsSays','Shitstatistssay','smuggies','SocialJusticeInAction','soyboys','The_Donald','TheNewRight','virginvschad','WatchRedditDie','weekendgunnit','WhereAreAllTheGoodMen','Wojak','YallCantBehave','yiffinhell','YouPostOnTheDonald','memes','PoliticalCompass','Showerthoughts','unpopularopinion','AnimalCrossing','Anticonsumption','antinatalism','AskFeminists','askgaybros','AskWomen','aspergers','aspiememes','ATBGE','australia','Bad_Cop_No_Donut','badlinguistics','bestoflegaladvice','BisexualTeens','blackmirror','blursedimages','bookscirclejerk','BoomerCringe','BPTmeta','brexit','brockhampton','bruhmoment','calvinandhobbes','CanadaPolitics','CapitalismVSocialism','casualiama','changemyview','clevercomebacks','ComedyCemetery','comicbooks','CoolBugFacts','d100','DankMemes_Circlejerk','DankPrecolumbianMemes','DDLC','DebateAnarchism','DebateCommunism','Dees_Nuts','Deltarune','democrats','DMAcademy','dndnext','dontdeadopeninside','EnoughLibertarianSpam','environment','europe','eurovision','EverythingScience','exchristian','exmormon','FacebookScience','factorio','FunnyandSad','furry','GenderCynical','gifs','Graffiti','gravityfalls','HighQualityGifs','HollowKnight','HollowKnightMemes','HydroHomies','ireland','Izlam','Jreg','JustBootThings','justneckbeardthings','kurzgesagt','LeopardsAteMyFace','lolgrindr','MadeOfStyrofoam','masstagger','melbourne','mew_irl','ModernPropaganda','NintendoSwitch','offbeat','OnePiece','OneWordBan','onewordeach','pan_media','PhilosophyMemes','pics','pokemon','PoliticalHumor','polyamory','popheads','ProJared','queencirclejerk','radiohead','radioheadcirclejerk','RoleReversal','rpg','rupaulsdragrace','Scotland','sex','shitduolingosays','shitpostemblem','ShitThe_DonaldSays','skeptic','SmashBrosUltimate','softwaregore','somnivexillology','splatoon','Stellaris','stevenuniverse','StrangerThings','suggestmeabook','tech','teenagersnew','thedavidpakmanshow','TheGoodPlace','transpassing','trees','TrueReddit','Trumpgret','u_bernie-sanders','Undertale','Unextexted','UrbanHell','User_Simulator','vinyl','walmart','Wellthatsucks','WhitePeopleTwitter','worldbuilding','worldjerking','worldpolitics','AAAAAAAAAAAAAAAAA','arabfunny','AskALiberal','AskEurope','AskTrumpSupporters','asoiaf','badhistory','berserklejerk','BikiniBottomTwitter','BoJackHorseman','boxoffice','Braincels','brasil','Breadit','CityPorn','CrappyDesign','DeuxRAMA','Documentaries','EndgameSpoilers','fakealbumcovers','fightporn','forhonor','freshalbumart','ftlgame','fuckepic','furry_irl','Games','Gamingdoublejerk','greentext','hearthstone','HongKong','iamatotalpieceofshit','IASIP','ich_iel','Israel','justlegbeardthings','Kenshi','lastimages','Liberal','magicTCG','MapPorn','memeingthroughtime','MonsterHunterWorld','NoStupidQuestions','nyc','patientgamers','polandball','Polska','rage','samharris','southpark','TerminallyStupid','The_Mueller','TheLetterH','TNOmod','tuesday','urbanplanning','videogamedunkey','Warthunder','Watchmen','WeirdWings','wow','YangForPresidentHQ','antifeminists','argentina','BanPitBulls','benshapiro','btd6','CallOfDuty','Classical_Liberals','climateskeptics','CombatFootage','dark_humor','financialindependence','forhonorknights','gundeals','hottiesfortrump','JordanPeterson','kotakuinaction2','LGBDropTheT','libtard','MemriTVmemes','menkampf','metro','NFA','POLITIC','ProtectAndServe','RoastMyCar','sabaton','Shuffles_Deck','SubforWhitePeopleOnly','The3rdPosition','TheLeftCantMeme','topnotchshitposting','TruePoliticalHumor','tucker_carlson','UnpopularFacts','vzla','walkaway','whiteknighting'])
    group by author, subreddit
);

-- ALTER TABLE predictions_comment_set ADD avg double precision;
-- UPDATE predictions_comment_set set avg = points::decimal/n;

ALTER TABLE predictions_comment_set ADD n_sub_type integer;
ALTER TABLE predictions_comment_set ADD avg_sub_type integer;
ALTER TABLE predictions_post_set ADD n_sub_type integer;
ALTER TABLE predictions_post_set ADD avg_sub_type integer;

update predictions_comment_set set n_sub_type = 0 where subreddit = ANY(ARRAY[]);
update predictions_comment_set set n_sub_type = 1 where subreddit = ANY(ARRAY[]);
update predictions_comment_set set n_sub_type = 2 where subreddit = ANY(ARRAY[]);
update predictions_comment_set set n_sub_type = 3 where subreddit = ANY(ARRAY[]);
....


update predictions_comment_set set points_sub_type = 0 where subreddit = ANY(ARRAY[]);
update predictions_comment_set set points_sub_type = 1 where subreddit = ANY(ARRAY[]);
update predictions_comment_set set points_sub_type = 2 where subreddit = ANY(ARRAY[]);
update predictions_comment_set set points_sub_type = 3 where subreddit = ANY(ARRAY[]);


-- nodejs script 1 will consume
select author, sum(n) as total_n, n_sub_type
from predictions_comment_set
group by (author,n_sub_type);

-- nodejs script 2 will consume
select author, sum(points)::decimal / sum(n) as total_ups, points_sub_type
from predictions_comment_set
group by (author,points_sub_type);


lib_n = 0
cent_n = 1
...
lib_ups = 8 -- avg

ALTER TABLE predictions_comment_set ADD n_comment_sub_type integer;


-- list of sub that are not in
CREATE INDEX idx_post_subreddit
on post(subreddit);

create table predictions_post_set as (
    select author, subreddit, count(*) as n, sum(score) as points
    from posts
    where created_utc > 1561153400 and
    subreddit = ANY(ARRAY['2meirl4meirl','aaaaaaacccccccce','ABoringDystopia','accidentallycommunist','AccidentalWesAnderson','actuallesbians','ADHD','AgainstHateSubreddits','agedlikemilk','ainbow','Anarchism','anarchocommunism','Anarchy101','AnCapMemes','AngrySocialist','ANI_COMMUNISM','AnimalsBeingBros','AnticommieCringe','AntifaHedgewik','AntifascistsofReddit','antifastonetoss','antiwork','AOC','AsABlackMan','asexuality','askphilosophy','asktransgender','assholedesign','AteTheOnion','atheism','Badfaketexts','BadMensAnatomy','badphilosophy','badwomensanatomy','BannedFromThe_Donald','beholdthemasterrace','bernieblindness','bestof','BestOfOutrageCulture','bi_irl','billwurtzmemes','bisexual','BlackPeopleTwitter','blessedimages','blunderyears','BoneAppleTea','bonehurtingjuice','boomershumor','BoomerTears','boottoobig','BrandNewSentence','bread_irl','BreadStapledToTrees','BreadTube','Breath_of_the_Wild','britishproblems','brooklynninenine','canadaleft','capitalism_in_decay','CasualUK','chaoticgood','ChapoTrapHouse','chapotraphouse2','chapotraphouse2_2_2','chomsky','circlebroke2','ClimateActionPlan','ClimateMemes','ClimateOffensive','comedyhomicide','comics','communism','communism101','CommunismMemes','COMPLETEANARCHY','conservativecartoons','ContraPoints','CorporateFacepalm','creepyPMs','CrewsCrew','Cultural_Marxism_irl','curlyhair','Cyberpunk','DankLeft','daverubin','DemocraticSocialism','depression','Destiny','discordapp','disneyvacation','dsa','EarthStrike','educationalgifs','egg_irl','ElizabethWarren','ENLIGHTENEDCENTRISM','ennnnnnnnnnnnbbbbbby','EnoughMuskSpam','enoughpetersonspam','EnoughTrumpSpam','entertainment','esist','ExtinctionRebellion','Eyebleach','fantanoforever','FanTheories','feemagers','Feminism','fireemblem','forwardsfromgrandma','forwardsfromhitler','ForwardsFromKlandma','FragileMaleRedditor','FragileWhiteRedditor','FrankOcean','freefolk','ftm','Fuckthealtright','FULLCOMMUNISM','gamegrumps','gameofthrones','GamerGhazi','Gamingcirclejerk','gatekeeping','gatesopencomeonin','gay','gay_irl','gaymers','GaySoundsShitposts','GenZanarchist','GenZedong','GetMotivated','GirlGamers','GoodFakeTexts','GreenAndPleasant','HailCorporate','happycowgifs','Hasan_Piker','hbomberguy','HelicopterAddict','hiphopheads','HumansBeingBros','iamverysmart','ifyoulikeblank','ihavesex','IncelTears','indieheads','InfowarriorRides','insaneparents','insanepeoplefacebook','InsanePeopleQuora','inspirobot','IronFrontUSA','IsTodayFridayThe13th','JacksFilms','JustLearnedTheFWord','Labour','LabourUK','LandlordLove','LateStageCapitalism','LateStageFeudalism','LateStageGenderBinary','LateStageImperialism','LeftieZ','LeftistGamersUnion','leftistvexillology','LeftWithoutEdge','lewronggeneration','lgbt','LGBTeens','LGBTnews','likeus','linkiscute','listentothis','lostgeneration','MadeMeSmile','magicthecirclejerking','MaliciousCompliance','mapporncirclejerk','MarchAgainstNazis','masterforgiveme','me_irl','me_irlgbt','meirl','mendrawingwomen','MensLib','menwritingwomen','MEOW_IRL','MoreTankieChapo','MovieDetails','movies','moviescirclejerk','MtF','Music','MutualSupport','mythologymemes','NetflixBestOf','niceguys','NobodyAsked','NonBinary','notdisneyvacation','nothingeverhappens','NotHowGirlsWork','NotKenM','notliketheothergirls','okboomerretard','OldSchoolCool','onejoke','onguardforthee','OurPresident','pansexual','PhilosophyTube','pointlesslygendered','Political_Revolution','politics','PragerUrine','PresidentialRaceMemes','PropagandaPosters','ProRevenge','punk','QueerVexillology','Qult_Headquarters','RadicalChristianity','rareinsults','reactiongifs','RedditInReddit','RedsKilledTrillions','religiousfruitcake','rickandmorty','RightCringe','rimjob_steve','rojava','RTGameCrowd','SampleSize','SandersForPresident','SapphoAndHerFriend','sbubby','ScottishPeopleTwitter','seculartalk','SelfAwarewolves','SequelMemes','ShitAmericansSay','shitfascistssay','ShitLiberalsSay','ShitMomGroupsSay','ShitRedditSays','shittymoviedetails','shittyrobots','Sigmarxism','Sino','sjws_bad','SmugIdeologyMan','socialism','Socialism_101','SocialistGaming','SocialistRA','SRAWeekend','StarWars','Stonetossingjuice','stopperpacks','StupidFood','SubredditSimMeta','SubSimulatorGPT2Meta','SubwayCreatures','suddenlybi','SuddenlyCommunist','SuddenlyGay','SuddenlyTrans','surrealmemes','suspiciouslyspecific','swoletariat','SyndiesUnited','TargetedShirts','TechNewsToday','technology','terriblefacebookmemes','terriblefandommemes','tf_irl','thanksimcured','ThatsHowThingsWork','The_Leftorium','THE_PACK','TheBluePill','TheOnion','therewasanattempt','TheRightCantMeme','ThisButUnironically','tipofmytongue','ToiletPaperUSA','TopMindsOfReddit','traaaaaaannnnnnnnnns','traaNSFW','trans','transgender','transgendercirclejerk','transpositive','transtimelines','TransyTalk','TrollCoping','TrollXChromosomes','TrumpCriticizesTrump','tumblr','TwoXChromosomes','u_userleansbot','UNBGBBIIVCHIDCTIICBG','UnethicalLifeProTips','unitedkingdom','VaushV','vaxxhappened','vegan','vegancirclejerk','vegetarian','VeryGay','vexillologycirclejerk','VoteBlue','WayOfTheBern','WhereAreTheChildren','wholesomebpt','wholesomememes','wholesomeyuri','WitchesVsPatriarchy','wlw_irl','worldnews','wowthanksimcured','YUROP','ZeroWaste','AnimalsBeingJerks','ANormalDayInRussia','Art','AskAnAmerican','austriahungary','badeconomics','Battlefield','BeAmazed','blackpeoplegifs','carporn','CasualConversation','centerleftpolitics','centrist','collapse','comedyamputation','CozyPlaces','Cringetopia','CryptoCurrency','Damnthatsinteresting','DankMemesFromSite19','dashcamgifs','dataisbeautiful','de','DeathtoAmeriKKKa','DebateAltRight','dirtbagcenter','DnD','dndmemes','dogelore','dogswithjobs','Drama','Enough_Sanders_Spam','EnoughCommieSpam','FellowKids','food','Futurology','gadgets','GamePhysics','GamersRiseUp','gaybros','geopolitics','gunpolitics','Hiphopcirclejerk','IAmA','imsorryjon','instant_regret','interestingasfuck','JustUnsubbed','Kaiserposting','Kaiserreich','KidsAreFuckingStupid','knives','KRCJ','LatinoPeopleTwitter','linguisticshumor','MakeupAddiction','mealtimevideos','megalophobia','MilitaryGfys','MilitaryPorn','Morrowind','MostBeautiful','NatureIsFuckingLit','neoconNWO','neoliberal','NewPatriotism','NoahGetTheBoat','nottheonion','okbuddyredacted','okmetaretard','OutOfTheLoop','paradoxplaza','pcgaming','perfectlycutscreams','Pete_Buttigieg','philosophy','quityourbullshit','rpghorrorstories','sadcringe','science','SCP','scriptedasiangifs','ShitPostCrusaders','shittyfoodporn','ShitWehraboosSay','space','SpeedOfLobsters','sports','starterpacks','subredditcancer','SubredditDrama','television','ThesaurizeThis','theyknew','ThingsCutInHalfPorn','thisismylifenow','tifu','TIHI','timeshiftedmemes','todayilearned','toptalent','trippinthroughtime','TumblrInAction','ukpolitics','videos','wallstreetbets','Watches','woahdude','woof_irl','WOSH','YangGang','youtubehaiku','4chan','AgainstDegenerateSubs','Anarcho_Capitalism','ape','Apustaja','ar15','askaconservative','AskLibertarians','AskThe_Donald','average_redditor','averageredditor','banned','BenShapiroMemes','Boomer','brasilivre','Capitalism','Catholicism','CatholicMemes','circlejerk','classic4chan','Conservative','conservatives','ConsumeProduct','CringeAnarchy','CrusadeMemes','CursedGuns','dankmemes','DarkHumorAndMemes','DarkJokeCentral','DeclineIntoCensorship','drumpfisfinished','edgydarkdankmemes','Firearms','FreeSpeech','frenworld','GenZ','GoldandBlack','Government_is_lame','GunPorn','guns','ImGoingToHellForThis','itsafetish','Jordan_Peterson_Memes','KotakuInAction','Libertarian','libertarianmeme','LouderWithCrowder','Megumin','MensRights','metacanada','MGTOW','Military','monarchism','MURICA','nattyorjuice','neogaianism','NoFap','NOWTTYG','pcmasterrace','PewdiepieSubmissions','Political_Tumor','PoliticalCompassMemes','progun','prolife','pussypassdenied','QualitySocialism','reclassified','Republican','RightwingLGBT','SargonofAkkad','shitguncontrollerssay','ShitNeoconsSay','ShitPoliticsSays','Shitstatistssay','smuggies','SocialJusticeInAction','soyboys','The_Donald','TheNewRight','virginvschad','WatchRedditDie','weekendgunnit','WhereAreAllTheGoodMen','Wojak','YallCantBehave','yiffinhell','YouPostOnTheDonald','memes','PoliticalCompass','Showerthoughts','unpopularopinion','AnimalCrossing','Anticonsumption','antinatalism','AskFeminists','askgaybros','AskWomen','aspergers','aspiememes','ATBGE','australia','Bad_Cop_No_Donut','badlinguistics','bestoflegaladvice','BisexualTeens','blackmirror','blursedimages','bookscirclejerk','BoomerCringe','BPTmeta','brexit','brockhampton','bruhmoment','calvinandhobbes','CanadaPolitics','CapitalismVSocialism','casualiama','changemyview','clevercomebacks','ComedyCemetery','comicbooks','CoolBugFacts','d100','DankMemes_Circlejerk','DankPrecolumbianMemes','DDLC','DebateAnarchism','DebateCommunism','Dees_Nuts','Deltarune','democrats','DMAcademy','dndnext','dontdeadopeninside','EnoughLibertarianSpam','environment','europe','eurovision','EverythingScience','exchristian','exmormon','FacebookScience','factorio','FunnyandSad','furry','GenderCynical','gifs','Graffiti','gravityfalls','HighQualityGifs','HollowKnight','HollowKnightMemes','HydroHomies','ireland','Izlam','Jreg','JustBootThings','justneckbeardthings','kurzgesagt','LeopardsAteMyFace','lolgrindr','MadeOfStyrofoam','masstagger','melbourne','mew_irl','ModernPropaganda','NintendoSwitch','offbeat','OnePiece','OneWordBan','onewordeach','pan_media','PhilosophyMemes','pics','pokemon','PoliticalHumor','polyamory','popheads','ProJared','queencirclejerk','radiohead','radioheadcirclejerk','RoleReversal','rpg','rupaulsdragrace','Scotland','sex','shitduolingosays','shitpostemblem','ShitThe_DonaldSays','skeptic','SmashBrosUltimate','softwaregore','somnivexillology','splatoon','Stellaris','stevenuniverse','StrangerThings','suggestmeabook','tech','teenagersnew','thedavidpakmanshow','TheGoodPlace','transpassing','trees','TrueReddit','Trumpgret','u_bernie-sanders','Undertale','Unextexted','UrbanHell','User_Simulator','vinyl','walmart','Wellthatsucks','WhitePeopleTwitter','worldbuilding','worldjerking','worldpolitics','AAAAAAAAAAAAAAAAA','arabfunny','AskALiberal','AskEurope','AskTrumpSupporters','asoiaf','badhistory','berserklejerk','BikiniBottomTwitter','BoJackHorseman','boxoffice','Braincels','brasil','Breadit','CityPorn','CrappyDesign','DeuxRAMA','Documentaries','EndgameSpoilers','fakealbumcovers','fightporn','forhonor','freshalbumart','ftlgame','fuckepic','furry_irl','Games','Gamingdoublejerk','greentext','hearthstone','HongKong','iamatotalpieceofshit','IASIP','ich_iel','Israel','justlegbeardthings','Kenshi','lastimages','Liberal','magicTCG','MapPorn','memeingthroughtime','MonsterHunterWorld','NoStupidQuestions','nyc','patientgamers','polandball','Polska','rage','samharris','southpark','TerminallyStupid','The_Mueller','TheLetterH','TNOmod','tuesday','urbanplanning','videogamedunkey','Warthunder','Watchmen','WeirdWings','wow','YangForPresidentHQ','antifeminists','argentina','BanPitBulls','benshapiro','btd6','CallOfDuty','Classical_Liberals','climateskeptics','CombatFootage','dark_humor','financialindependence','forhonorknights','gundeals','hottiesfortrump','JordanPeterson','kotakuinaction2','LGBDropTheT','libtard','MemriTVmemes','menkampf','metro','NFA','POLITIC','ProtectAndServe','RoastMyCar','sabaton','Shuffles_Deck','SubforWhitePeopleOnly','The3rdPosition','TheLeftCantMeme','topnotchshitposting','TruePoliticalHumor','tucker_carlson','UnpopularFacts','vzla','walkaway','whiteknighting'])
    group by author, subreddit
);



ALTER TABLE predictions_comment_set ADD density decimal;
ALTER TABLE predictions_post_set ADD density decimal;

with list_of_author as (
    SELECT distinct(author)
    FROM (
        select distinct(author) as author  from predictions_comment_set
        UNION
        select distinct(author) as author from predictions_post_set
    ) as all_author
)

update table  predictions_post_set
set density = n::decimal/author_pc.count_of_posts::decimal
from author_post_count as author_pc
where predictions_post_set.author = author_pc.author;


create table author_comment_count as (
    select author,count(*) as count_of_comments
    from comment 
    where created_utc > 1561153400
    and subreddit in (select distinct(author) as author  from predictions_comment_set)
    group by author
);



create table author_post_count as (
    with list_of_author as (
        SELECT distinct(author)
        FROM (
            select distinct(author) as author  from predictions_comment_set
            UNION
            select distinct(author) as author from predictions_post_set
        ) as all_author
    )
    select author,count(*) as count_of_posts
    from posts 
    where created_utc > 1561153400
    group by author
);


create table all_comment_aggr as (
    select author, subreddit, count(*) as n
    from comment
    where created_utc > 1561153400 
    group by author, subreddit
);

create table all_post_aggr as (
    select author, subreddit, count(*) as n
    from posts
    where created_utc > 1561153400 
    group by author, subreddit
);

CREATE INDEX idx_author_acc on author_comment_count(author);
CREATE INDEX idx_author_apc on author_post_count(author);

ALTER TABLE all_comment_aggr ADD density decimal;
ALTER TABLE all_post_aggr ADD density decimal;

update table all_comment_aggr
set density = n::decimal/author_cc.count_of_comments::decimal
from author_comment_count as author_cc
where all_comment_aggr.author = author_cc.author;

update table all_post_aggr
set density = n::decimal/author_pc.count_of_comments::decimal
from author_post_count as author_pc
where all_comment_aggr.author = author_pc.author;

-- median
SELECT author,
       PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY density) as aggr_density
FROM all_comment_aggr
GROUP BY author;
SELECT author,
       PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY density) as aggr_density
FROM all_post_aggr
GROUP BY author;

-- avg
SELECT author,
       avg(density) as aggr_density
FROM all_comment_aggr
GROUP BY author;
SELECT author,
       avg(density) as aggr_density
FROM all_post_aggr
GROUP BY author;

-- avg n
create table ranking_density_comment as (
    SELECT author,
        subreddit
        density,
        RANK() OVER(
            PARTITION BY author
            ORDER BY density DESC
        ) as rank_order,
        ROW_NUMBER() OVER(
            PARTITION BY author
            ORDER BY density DESC
        ) as count_num
    FROM all_comment_aggr
);
select r.author, avg(r.density) as aggr_density
FROM  ranking_density_comment r
WHERE r.count_num <= 3
GROUP BY r.author;

create table ranking_density_post as (
    SELECT author,
        subreddit
        density,
        RANK() OVER(
            PARTITION BY author
            ORDER BY density DESC
        ) as rank_order,
        ROW_NUMBER() OVER(
            PARTITION BY author
            ORDER BY density DESC
        ) as count_num
    FROM all_post_aggr
);
select r.author, avg(r.density) as aggr_density
FROM  ranking_density_post r
WHERE r.count_num <= 3
GROUP BY r.author;