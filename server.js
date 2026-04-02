const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

// =============================
// CONFIG
// =============================
const MAX_SUBJECT_RETRIES = 3;
const PROCESS_INTERVAL_MS = 1500; // Relaxed interval — reduces HubSpot API burst pressure
const CONCURRENCY = 5;            // Reduced from 10 — this nurture makes 4-5 HubSpot calls per contact

const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// =============================
// DOMAIN SCRAPE CACHE
// Scrape each domain once per server session, reuse for all contacts at that domain
// =============================
const domainCache = new Map();
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour TTL

// Domains known to block scrapers — skip immediately
const BLOCKED_DOMAINS = new Set([
  'bankofamerica.com', 'wellsfargo.com', 'citigroup.com', 'citi.com',
  'chase.com', 'jpmorgan.com', 'goldmansachs.com', 'morganstanley.com',
  'herbalife.com', 'securityfinance.com', 'braze.com', 'salesforce.com',
  'microsoft.com', 'google.com', 'amazon.com', 'apple.com', 'meta.com',
  'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com'
]);

// =============================
// INDUSTRY CASE STUDY LINKS
// =============================
const HOTEL_CASE_STUDY = 'https://www.pedowitzgroup.com/case-studies/fourseasons';

const SPORTS_CASE_STUDIES = [
  'https://www.pedowitzgroup.com/case-studies/trailblazers',
  'https://www.pedowitzgroup.com/case-studies/tdgarden',
  'https://www.pedowitzgroup.com/case-studies/timberwolves',
  'https://www.pedowitzgroup.com/case-studies/jazz',
  'https://www.pedowitzgroup.com/case-studies/warriors',
  'https://www.pedowitzgroup.com/case-studies/miamiheat',
  'https://www.pedowitzgroup.com/case-studies/cavaliers'
];

function getCaseStudyLink(industryCategory) {
  const cat = (industryCategory || '').toLowerCase();
  if (cat.includes('hotel') || cat.includes('lodging')) return HOTEL_CASE_STUDY;
  if (cat.includes('sport')) return SPORTS_CASE_STUDIES[Math.floor(Math.random() * SPORTS_CASE_STUDIES.length)];
  return HOTEL_CASE_STUDY;
}

// =============================
// HUBSPOT SERVICE LIBRARY
// =============================
const TPG_HUBSPOT_SERVICES = [
  {
    label: "HubSpot CRM Setup & Optimization",
    url: "https://www.pedowitzgroup.com/hubspot-crm",
    pain: "Most hospitality and sports organizations using HubSpot are only scratching the surface of the CRM. Pipeline visibility is murky, contact data is fragmented across venues and properties, and reporting doesn't tell the full story. We fix the foundation.",
    angle: "CRM optimization and pipeline visibility"
  },
  {
    label: "HubSpot Demand Generation",
    url: "https://www.pedowitzgroup.com/hubspot-demand-generation",
    pain: "Having HubSpot and generating real pipeline from it are two different things. We build multi-channel demand programs inside HubSpot tuned for hospitality and sports audiences that convert, not just campaigns that look busy.",
    angle: "pipeline generation and lead conversion"
  },
  {
    label: "HubSpot Marketing Automation & Workflows",
    url: "https://www.pedowitzgroup.com/hubspot-run-it",
    pain: "Growing hospitality and sports teams with HubSpot often hit a wall. Workflows break, leads fall through cracks, and the automation that was supposed to save time starts creating more work. We rebuild it right.",
    angle: "workflow automation and operational efficiency"
  },
  {
    label: "HubSpot Sales Enablement",
    url: "https://www.pedowitzgroup.com/hubspot-sales-enablement",
    pain: "HubSpot has powerful sales tools that most hospitality and sports teams never fully activate — sequences, playbooks, deal scoring, and coaching insights. We set them up so your team closes faster with less friction.",
    angle: "sales productivity and deal velocity"
  },
  {
    label: "HubSpot Platform Migration",
    url: "https://www.pedowitzgroup.com/hubspot-move-it",
    pain: "If your team is straddling HubSpot and a legacy MAP like Pardot or Marketo, you are paying for two systems and getting half the results from both. We have done 1,000+ migrations. Clean, fast, zero data loss.",
    angle: "platform consolidation and MarTech simplification"
  },
  {
    label: "HubSpot Creative & Content",
    url: "https://www.pedowitzgroup.com/hubspot-creative-and-content",
    pain: "Content that resonates with hotel guests and sports fans is different from generic B2B copy. TPG builds HubSpot-native content strategies that speak to your specific audiences and drive real engagement.",
    angle: "content strategy and creative execution"
  },
  {
    label: "HubSpot Website & CMS",
    url: "https://www.pedowitzgroup.com/hubspot-website",
    pain: "A HubSpot website that is not built for conversion is just a digital brochure. We design and build CMS Hub experiences that capture leads, personalize by audience segment, and tie every visit back to revenue.",
    angle: "website conversion and CMS optimization"
  },
  {
    label: "HubSpot Tune-It (Optimization & Audit)",
    url: "https://www.pedowitzgroup.com/hubspot-tune-it",
    pain: "Most organizations have been using HubSpot for a year or more without a true audit. Properties are misconfigured, workflows are redundant, and the portal is drifting from your actual business process. We tune it back to peak performance.",
    angle: "HubSpot portal audit and optimization"
  },
  {
    label: "HubSpot Managed Services",
    url: "https://www.pedowitzgroup.com/hubspot-main",
    pain: "As your team grows and adds headcount in Marketing and RevOps, the question is not just who owns HubSpot. It is who is optimizing it. TPG acts as your on-call HubSpot team for strategy, execution, and continuous improvement.",
    angle: "ongoing HubSpot management and optimization"
  },
  {
    label: "HubSpot Run-It (Full Execution Services)",
    url: "https://www.pedowitzgroup.com/hubspot-run-it",
    pain: "Strategy without execution is just a deck. TPG runs HubSpot campaigns, automations, and programs end-to-end for hospitality and sports brands that want results without adding headcount.",
    angle: "full-service HubSpot program execution"
  }
];

// =============================
// INTENT TOPIC CONTEXT MAP
// =============================
const INTENT_TOPIC_CONTEXT = {
  'customer insight': {
    angle: 'unlocking deeper customer intelligence',
    pain: 'You searched for customer insight solutions. Most organizations have a goldmine of behavioral and transactional data that never gets turned into actionable intelligence. HubSpot is built to change that, and TPG builds the reporting and segmentation infrastructure that makes it real.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-crm'
  },
  'customer engagement': {
    angle: 'driving deeper engagement across every touchpoint',
    pain: 'You have been researching customer engagement tools. HubSpot is built for exactly this — workflows, sequences, personalization, and lifecycle nurture. TPG activates the full stack for organizations in your industry.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-run-it'
  },
  'marketing automation tools': {
    angle: 'building automation that actually scales',
    pain: 'You have been evaluating marketing automation platforms. HubSpot is the platform we use with clients in your industry to build automation that runs programs without adding headcount. TPG has set this up for organizations just like yours.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-run-it'
  },
  'sales automation': {
    angle: 'automating your sales process end-to-end',
    pain: 'Your search for sales automation tells us your team is spending too much time on manual tasks. HubSpot Sales Hub has sequences, playbooks, and deal automation that eliminate that friction. We set it up for teams in your industry every week.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-sales-enablement'
  },
  'crm software': {
    angle: 'finding and getting full value from the right CRM',
    pain: 'You have been researching CRM software. HubSpot is what we recommend and implement for organizations in your industry because it unifies your marketing, sales, and service data in one place. TPG handles the full evaluation and setup.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-crm'
  },
  'customer journey': {
    angle: 'mapping and activating the full customer journey',
    pain: 'Customer journey was your search focus. HubSpot is purpose-built to orchestrate every stage of the journey. TPG builds the full connected journey for organizations in your industry, from first touch to loyal repeat customer.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-demand-generation'
  },
  'marketing technology': {
    angle: 'simplifying and maximizing your MarTech stack',
    pain: 'You have been evaluating marketing technology. The most common finding we see is too many tools, not enough integration. TPG helps organizations in your industry evaluate, consolidate around HubSpot, and eliminate the stack complexity draining budget and attention.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-move-it'
  },
  'crm': {
    angle: 'building a CRM that becomes your revenue foundation',
    pain: 'CRM was top of mind in your research. The difference between a CRM that creates visibility and one that creates more work is almost entirely in the platform choice and setup strategy. TPG has implemented HubSpot CRM for 1,300+ clients.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-crm'
  },
  'customer journey management': {
    angle: 'managing and automating the full customer lifecycle',
    pain: 'You searched for customer journey management. HubSpot is built to automate the entire lifecycle but only if it is set up correctly from the start. TPG builds the triggers, workflows, and personalization layers that make it run without manual intervention.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-tune-it'
  },
  'lead management': {
    angle: 'building a lead management engine that never leaks',
    pain: 'Lead management was your focus. In hospitality and sports, leads come from multiple channels and properties. HubSpot can unify and route all of them, but only if the architecture is built correctly from day one. TPG does this every week.',
    serviceUrlHint: 'https://www.pedowitzgroup.com/hubspot-demand-generation'
  }
};

function getIntentContext(intentTopicSearched) {
  if (!intentTopicSearched) return null;
  const key = intentTopicSearched.toLowerCase().trim();
  return INTENT_TOPIC_CONTEXT[key] || null;
}

// =============================
// QUEUE, CONCURRENCY & ERROR TRACKING
// =============================
let queue = [];
let inFlight = 0;
let errorCount = 0;

// =============================
// HEALTH CHECK
// =============================
app.get("/", (req, res) => {
  res.json({
    status: "ok",
    queueLength: queue.length,
    inFlight: inFlight,
    concurrency: CONCURRENCY,
    errorCount
  });
});

// =============================
// LIVE DASHBOARD
// =============================
app.get("/dashboard", (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Industry HubSpot Nurture &mdash; Scott Benedetti</title>
      <meta http-equiv="refresh" content="5">
      <style>
        body { font-family: monospace; background: #0f0f0f; color: #a2cf23; padding: 40px; }
        h1 { font-size: 18px; margin-bottom: 6px; color: #fff; }
        h2 { font-size: 13px; color: #555; margin: 0 0 30px 0; font-weight: normal; }
        .grid { display: flex; gap: 60px; margin-bottom: 40px; }
        .stat { font-size: 64px; font-weight: bold; margin: 0; line-height: 1; }
        .label { font-size: 13px; color: #555; margin-top: 8px; }
        .green { color: #a2cf23; }
        .orange { color: #f0a500; }
        .red { color: #e05252; }
        .grey { color: #333; }
        .footer { font-size: 12px; color: #333; margin-top: 40px; border-top: 1px solid #1a1a1a; padding-top: 20px; }
      </style>
    </head>
    <body>
      <h1>Industry HubSpot Nurture &mdash; Scott Benedetti</h1>
      <h2>Audience: Hotel/Lodging &amp; Sports Teams &nbsp;|&nbsp; HubSpot Intent Signals</h2>
      <div class="grid">
        <div class="block">
          <div class="stat ${queue.length > 0 ? 'orange' : 'grey'}">${queue.length}</div>
          <div class="label">contacts waiting in queue</div>
        </div>
        <div class="block">
          <div class="stat green">${inFlight}</div>
          <div class="label">in-flight (of ${CONCURRENCY} max slots)</div>
        </div>
        <div class="block">
          <div class="stat green">${CONCURRENCY - inFlight}</div>
          <div class="label">open slots available</div>
        </div>
        <div class="block">
          <div class="stat ${errorCount > 0 ? 'red' : 'grey'}">${errorCount}</div>
          <div class="label">processing errors (since last restart)</div>
        </div>
      </div>
      <div class="footer">
        Last refreshed: ${new Date().toLocaleTimeString()} &nbsp;·&nbsp; Auto-refreshes every 5 seconds
      </div>
    </body>
    </html>
  `);
});

// =============================
// ENQUEUE FROM HUBSPOT
// =============================
app.post("/enqueue", (req, res) => {
  queue.push({ ...req.body, retries: 0 });
  res.status(200).json({
    status: "queued",
    queuePosition: queue.length
  });
});

// =============================
// PROCESS A SINGLE JOB
// =============================
async function processJob(job) {
  try {
    await updateStatus(job.contactId, "IN_PROGRESS");

    // Fetch industry_category and intent_topic_searched from HubSpot
    const contactProps = await fetchContactProperties(job.contactId);
    job.industry_category = contactProps.industry_category || job.industry_category || '';
    job.intent_topic_searched = contactProps.intent_topic_searched || job.intent_topic_searched || '';

    const result = await runClaude(job);

    await writeResults(job.contactId, result, job.sequenceStep || 1);
    await updateStatus(job.contactId, "SENT");

    console.log(`✅ Completed: ${job.contactId} - Step ${job.sequenceStep} | Industry: ${job.industry_category} | Intent: ${job.intent_topic_searched}`);
  } catch (err) {
    console.error(`❌ Error for ${job.contactId}:`, err.message);

    if (err.response?.status === 429) {
      console.log(`⏳ Rate limited, requeuing ${job.contactId}`);
      queue.push(job);
    } else {
      job.retries = (job.retries || 0) + 1;
      if (job.retries <= 2) {
        await updateStatus(job.contactId, "RETRY_PENDING");
        queue.push(job);
      } else {
        errorCount++;
        await updateStatus(job.contactId, "FAILED");
      }
    }
  } finally {
    inFlight--;
  }
}

// =============================
// FETCH CONTACT PROPERTIES FROM HUBSPOT
// =============================
async function fetchContactProperties(contactId) {
  try {
    const res = await axios.get(
      `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
      {
        params: { properties: 'industry_category,intent_topic_searched' },
        headers: {
          Authorization: `Bearer ${HUBSPOT_TOKEN}`,
          'Content-Type': 'application/json'
        },
        timeout: 8000
      }
    );
    return res.data?.properties || {};
  } catch (err) {
    console.error(`⚠️ Could not fetch HubSpot props for ${contactId}:`, err.message);
    return {};
  }
}

// =============================
// WORKER LOOP — CONCURRENT BATCH
// =============================
setInterval(() => {
  while (inFlight < CONCURRENCY && queue.length > 0) {
    const job = queue.shift();
    inFlight++;
    processJob(job);
  }
  if (queue.length > 0 || inFlight > 0) {
    console.log(`📊 Queue: ${queue.length} | In-flight: ${inFlight}`);
  }
}, PROCESS_INTERVAL_MS);

// =============================
// URL NORMALIZER
// =============================
function normalizeUrl(rawUrl) {
  if (!rawUrl) return null;
  let url = rawUrl.trim().replace(/\/+$/, '');
  if (!/^https?:\/\//i.test(url)) url = 'https://' + url;
  try { new URL(url); return url; } catch { return null; }
}

// =============================
// HTML STRIPPER
// =============================
function stripHtml(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// =============================
// TITLE EXTRACTION
// =============================
async function extractTitles(url) {
  const normalized = normalizeUrl(url);
  if (!normalized) return [];
  try {
    const res = await axios.get(normalized, {
      timeout: 4000,
      maxContentLength: 300000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9'
      }
    });
    const html = res.data || '';
    const headlineMatches = [];
    const headingRegex = /<h[12][^>]*>([\s\S]*?)<\/h[12]>/gi;
    let match;
    while ((match = headingRegex.exec(html)) !== null) {
      const text = match[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
      if (text.length >= 20 && text.length <= 160) headlineMatches.push(text);
    }
    if (headlineMatches.length >= 2) return headlineMatches.slice(0, 3);
    const text = stripHtml(html);
    const fallbackMatches = [...text.matchAll(/(.{25,120})\s+(20\d{2})/g)];
    return fallbackMatches.map(m => m[1].trim()).slice(0, 3);
  } catch (err) {
    const reason = err.code === 'ECONNABORTED' ? 'timeout'
      : err.response ? `HTTP ${err.response.status}` : err.message;
    console.log(`⚠️ Could not fetch ${normalized}: ${reason}`);
    return [];
  }
}

// =============================
// DEEP COMPANY RESEARCH — WITH DOMAIN CACHE
// Scrapes homepage, about, leadership, news, and blog pages
// =============================
async function getCompanyContent(website) {
  const baseUrl = normalizeUrl(website);
  if (!baseUrl) return { newsBlock: null, blogBlock: null, homepageBlock: null, aboutBlock: null, leadershipBlock: null };

  let domain = '';
  try {
    domain = new URL(baseUrl).hostname.replace(/^www\./, '');
  } catch {
    return { newsBlock: null, blogBlock: null, homepageBlock: null, aboutBlock: null, leadershipBlock: null };
  }

  if (BLOCKED_DOMAINS.has(domain)) {
    console.log(`🚫 Skipping blocked domain: ${domain}`);
    return { newsBlock: null, blogBlock: null, homepageBlock: null, aboutBlock: null, leadershipBlock: null };
  }

  if (domainCache.has(domain)) {
    const cached = domainCache.get(domain);
    if (Date.now() - cached.cachedAt < CACHE_TTL_MS) {
      console.log(`💾 Cache hit: ${domain}`);
      return {
        newsBlock: cached.newsBlock,
        blogBlock: cached.blogBlock,
        homepageBlock: cached.homepageBlock,
        aboutBlock: cached.aboutBlock,
        leadershipBlock: cached.leadershipBlock
      };
    }
  }

  const newsPaths = ['/news', '/press', '/newsroom', '/press-releases', '/company-news', '/about/news', '/awards', '/recognition'];
  const blogPaths = ['/blog', '/insights', '/resources', '/thought-leadership', '/articles'];
  const aboutPaths = ['/about', '/about-us', '/our-story', '/company', '/who-we-are'];
  const leadershipPaths = ['/leadership', '/team', '/about/leadership', '/about/team', '/our-team', '/management'];

  let newsBlock = null, blogBlock = null, homepageBlock = null, aboutBlock = null, leadershipBlock = null;

  // HOMEPAGE
  try {
    const res = await axios.get(baseUrl, {
      timeout: 5000, maxContentLength: 400000,
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml', 'Accept-Language': 'en-US,en;q=0.9' }
    });
    const html = res.data || '';
    const metaMatch = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']{20,300})["']/i)
      || html.match(/<meta[^>]+content=["']([^"']{20,300})["'][^>]+name=["']description["']/i);
    const metaDesc = metaMatch ? metaMatch[1].trim() : null;
    const h1Match = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
    const h1Text = h1Match ? h1Match[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim() : null;
    const h2Regex = /<h2[^>]*>([\s\S]*?)<\/h2>/gi;
    const h2Texts = [];
    let h2m;
    while ((h2m = h2Regex.exec(html)) !== null) {
      const t = h2m[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
      if (t.length >= 10 && t.length <= 150) h2Texts.push(t);
      if (h2Texts.length >= 4) break;
    }
    const parts = [];
    if (h1Text && h1Text.length >= 10) parts.push(`Homepage headline: "${h1Text}"`);
    if (metaDesc) parts.push(`Site description: "${metaDesc}"`);
    if (h2Texts.length) parts.push(`Key homepage sections: ${h2Texts.map(t => `"${t}"`).join(', ')}`);
    if (parts.length) {
      homepageBlock = `HOMEPAGE INTELLIGENCE (VERIFIED from ${baseUrl}):\n` + parts.join('\n');
      console.log(`🏠 Homepage scraped: ${domain}`);
    }
  } catch (err) {
    console.log(`⚠️ Homepage scrape failed for ${domain}: ${err.message}`);
  }

  // ABOUT PAGE
  for (const path of aboutPaths) {
    try {
      const res = await axios.get(`${baseUrl}${path}`, {
        timeout: 4000, maxContentLength: 300000,
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html' }
      });
      const text = stripHtml(res.data || '');
      const sentences = text.match(/[A-Z][^.!?]{49,199}[.!?]/g) || [];
      const relevant = sentences.filter(s => !s.toLowerCase().includes('cookie') && !s.toLowerCase().includes('privacy')).slice(0, 4);
      if (relevant.length >= 2) {
        aboutBlock = `ABOUT / COMPANY STORY (VERIFIED from ${baseUrl}${path}):\n` + relevant.map(s => `- ${s.trim()}`).join('\n');
        console.log(`📖 About page scraped: ${domain}${path}`);
        break;
      }
    } catch { /* try next */ }
  }

  // LEADERSHIP PAGE
  for (const path of leadershipPaths) {
    try {
      const res = await axios.get(`${baseUrl}${path}`, {
        timeout: 4000, maxContentLength: 300000,
        headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html' }
      });
      const html = res.data || '';
      const nameRegex = /<h[2-4][^>]*>([\s\S]*?)<\/h[2-4]>/gi;
      const names = [];
      let nm;
      while ((nm = nameRegex.exec(html)) !== null) {
        const t = nm[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
        if (/^[A-Z][a-z]+(?: [A-Z][a-z]+){1,3}$/.test(t)) names.push(t);
        if (names.length >= 5) break;
      }
      if (names.length >= 2) {
        leadershipBlock = `LEADERSHIP TEAM (VERIFIED from ${baseUrl}${path}):\n` + names.map(n => `- ${n}`).join('\n');
        console.log(`👤 Leadership scraped: ${domain}${path}`);
        break;
      }
    } catch { /* try next */ }
  }

  // NEWS
  for (const path of newsPaths) {
    const titles = await extractTitles(`${baseUrl}${path}`);
    if (titles.length >= 1) {
      newsBlock = `COMPANY NEWS & RECENT ANNOUNCEMENTS (VERIFIED from ${baseUrl}${path}):\n` + titles.map(t => `- ${t}`).join('\n');
      console.log(`📰 News scraped: ${baseUrl}${path}`);
      break;
    }
  }

  // BLOG
  for (const path of blogPaths) {
    const titles = await extractTitles(`${baseUrl}${path}`);
    if (titles.length >= 1) {
      blogBlock = `CONTENT & THOUGHT LEADERSHIP (VERIFIED from ${baseUrl}${path}):\n` + titles.map(t => `- ${t}`).join('\n');
      console.log(`📝 Blog scraped: ${baseUrl}${path}`);
      break;
    }
  }

  domainCache.set(domain, { newsBlock, blogBlock, homepageBlock, aboutBlock, leadershipBlock, cachedAt: Date.now() });
  console.log(`💾 Cached: ${domain} (${domainCache.size} domains in cache)`);

  return { newsBlock, blogBlock, homepageBlock, aboutBlock, leadershipBlock };
}

// =============================
// INDUSTRY PROFILE BUILDER
// =============================
function buildIndustryProfile(industryCategory) {
  const cat = (industryCategory || '').toLowerCase();
  const isHotel = cat.includes('hotel') || cat.includes('lodging');
  const isSports = cat.includes('sport');

  if (isHotel) {
    return {
      industryLabel: 'Hotel & Lodging',
      audienceContext: 'hospitality marketing and revenue leaders managing guest acquisition, loyalty programs, group sales, and brand reputation across properties',
      hubspotAngle: 'HubSpot is increasingly the platform of choice for forward-thinking hospitality brands to unify guest data, automate pre-arrival and post-stay communications, and build loyalty programs that drive repeat bookings',
      painPoints: [
        'fragmented guest data spread across PMS, OTAs, and CRM with no unified view',
        'manual pre-arrival and post-stay communications that should be automated',
        'group sales pipelines tracked in spreadsheets instead of a proper CRM',
        'loyalty and guest retention programs not connected to lifecycle automation',
        'marketing attribution that cannot connect a campaign to an actual booking'
      ],
      toneGuidance: 'Write as someone who deeply understands the hospitality business. Reference concepts like RevPAR, ADR, group blocks, pre-arrival sequences, loyalty tiers, OTA dependence, and direct booking strategy naturally.'
    };
  } else if (isSports) {
    return {
      industryLabel: 'Sports Team / Sports Organization',
      audienceContext: 'sports marketing and business development leaders managing ticket sales, sponsorship pipelines, fan engagement, season ticket renewal, and corporate partnership revenue',
      hubspotAngle: 'HubSpot is the modern platform for sports organizations that want to unify fan and sponsor data, automate renewal and upsell campaigns, and build personalized journeys from first game to multi-year package holder',
      painPoints: [
        'season ticket renewal campaigns that are still manually segmented and sent',
        'sponsorship and corporate partnership pipelines living in spreadsheets',
        'fan engagement data siloed between ticketing platforms, email, and the CRM',
        'no automated journey from single-game buyer to full season ticket holder',
        'limited ability to attribute marketing campaigns to actual ticket revenue'
      ],
      toneGuidance: 'Write as someone who knows the sports business. Reference concepts like season ticket renewals, partial plans, group sales, sponsorship activation, fan lifetime value, and corporate suite packages naturally.'
    };
  }

  return {
    industryLabel: industryCategory || 'your industry',
    audienceContext: 'marketing and revenue leaders looking to drive growth',
    hubspotAngle: 'HubSpot connects marketing, sales, and service data into one revenue engine',
    painPoints: ['fragmented data across multiple tools', 'manual processes that should be automated', 'poor pipeline visibility and attribution'],
    toneGuidance: 'Write as a trusted HubSpot expert who has seen and solved these problems hundreds of times.'
  };
}

// =============================
// DASH REMOVER
// =============================
function removeDashes(text) {
  return text.replace(/\s*—\s*/g, ', ').replace(/\s*–\s*/g, ', ').replace(/  +/g, ' ').trim();
}

// =============================
// SIGNATURE REMOVER
// =============================
function removeSignature(text) {
  return text
    .replace(/\n+\s*(Jeff|Scott)\s*$/i, '')
    .replace(/\n+\s*(Best|Best regards|Thanks|Thank you|Regards|Sincerely|Cheers|Warm regards)[^\n]*/gi, '')
    .trim();
}

// =============================
// CLAUDE LOGIC
// =============================
async function runClaude(job) {
  const SEQUENCE_STEP = job.sequenceStep || 1;

  const {
    firstname = '',
    company = '',
    jobtitle = '',
    industry = '',
    industry_category = '',
    intent_topic_searched = '',
    numemployees = '',
    annualrevenue = '',
    hs_linkedin_url = '',
    website = '',
    web_technologies = '',
    description = '',
    hs_analytics_last_url = '',
    hs_analytics_num_page_views = ''
  } = job;

  const industryProfile = buildIndustryProfile(industry_category);
  const intentContext = getIntentContext(intent_topic_searched);

  // Case study gating
  const isHotelContact = industry_category.toLowerCase().includes('hotel') || industry_category.toLowerCase().includes('lodging');
  const HOTEL_CASE_STUDY_STEPS = [1, 4, 8];
  const includeCaseStudy = isHotelContact ? HOTEL_CASE_STUDY_STEPS.includes(SEQUENCE_STEP) : true;
  const caseStudyUrl = getCaseStudyLink(industry_category);
  const caseStudyLabel = isHotelContact ? 'Four Seasons case study' : 'one of our sports client case studies';

  // Behavioral signals
  let behavioralContext = '';
  const pageViews = parseInt(hs_analytics_num_page_views) || 0;
  const lastUrl = hs_analytics_last_url ? hs_analytics_last_url.trim() : '';
  if (pageViews >= 10) behavioralContext += `High website engagement (${pageViews} pages viewed). `;
  else if (pageViews >= 5) behavioralContext += `Moderate website engagement (${pageViews} pages). `;
  else if (pageViews >= 1) behavioralContext += `Initial website visit (${pageViews} pages). `;
  else behavioralContext += `No prior website visits detected. `;
  if (lastUrl) {
    const url = lastUrl.toLowerCase();
    if (url.includes('/pricing')) behavioralContext += 'Viewed pricing — evaluating investment.';
    else if (url.includes('/demo') || url.includes('/get-started')) behavioralContext += 'Visited demo/get-started — high intent.';
    else if (url.includes('/case-stud') || url.includes('/customer')) behavioralContext += 'Reviewed case studies — seeking proof points.';
    else if (url.includes('/hubspot')) behavioralContext += 'Specifically researched HubSpot solutions.';
    else if (url.includes('/blog') || url.includes('/resource')) behavioralContext += 'Consumed content — educational phase.';
    else {
      const pageName = lastUrl.split('/').filter(p => p).pop()?.replace(/-/g, ' ') || 'homepage';
      behavioralContext += `Last viewed: ${pageName}.`;
    }
  }
  const BehavioralContext = behavioralContext.trim() || 'No behavioral data available.';

  // Service for this step
  const serviceIndex = (SEQUENCE_STEP - 1) % TPG_HUBSPOT_SERVICES.length;
  const featuredService = TPG_HUBSPOT_SERVICES[serviceIndex];

  // Prior emails
  let priorEmailsText = [];
  for (let i = 1; i < SEQUENCE_STEP; i++) {
    const field = job[`industry_hubspot_nurture_claude_text_em${i}`];
    if (field) priorEmailsText.push(`EMAIL ${i}:\n${field}`);
  }
  const priorEmailsBlock = priorEmailsText.length ? priorEmailsText.join("\n\n---\n\n") : "N/A";

  // Deep company research
  let companyNewsBlock       = `COMPANY NEWS & RECENT ANNOUNCEMENTS:\n- None found`;
  let companyContentBlock    = `CONTENT & THOUGHT LEADERSHIP:\n- None found`;
  let companyHomepageBlock   = `HOMEPAGE INTELLIGENCE:\n- None found`;
  let companyAboutBlock      = `ABOUT / COMPANY STORY:\n- None found`;
  let companyLeadershipBlock = `LEADERSHIP TEAM:\n- None found`;

  if (website) {
    try {
      const { newsBlock, blogBlock, homepageBlock, aboutBlock, leadershipBlock } = await getCompanyContent(website);
      if (newsBlock)       companyNewsBlock       = newsBlock;
      if (blogBlock)       companyContentBlock    = blogBlock;
      if (homepageBlock)   companyHomepageBlock   = homepageBlock;
      if (aboutBlock)      companyAboutBlock      = aboutBlock;
      if (leadershipBlock) companyLeadershipBlock = leadershipBlock;
    } catch (err) {
      console.log(`⚠️ Research failed for ${company}: ${err.message}`);
    }
  }

  const companyIntelligenceBrief = [
    companyHomepageBlock,
    companyAboutBlock,
    companyLeadershipBlock,
    companyNewsBlock,
    companyContentBlock
  ].join('\n\n');

  // Intent block
  const intentBlock = intentContext
    ? `INTENT SIGNAL — WHAT THEY SEARCHED FOR:
- Topic Searched: "${intent_topic_searched}"
- Angle: ${intentContext.angle}
- Pain This Signals: ${intentContext.pain}
- Suggested Service URL for this intent: ${intentContext.serviceUrlHint}
IMPORTANT: This contact searched for "${intent_topic_searched}". This is your most powerful personalization hook. Open with or reference this search intent naturally to show you understand what they are trying to solve.`
    : `INTENT SIGNAL: No specific intent topic available. Use company intelligence and behavioral signals as your primary hooks.`;

  const userContent = `You are Scott Benedetti, Partner and Executive Vice President of The Pedowitz Group (TPG), writing EMAIL ${SEQUENCE_STEP} in a 10-touch personalized outbound nurture sequence.

YOUR SINGLE GOAL:
Get ${firstname} at ${company} to book a meeting with Scott. Every word in this email must serve that goal.

This email must read like Scott personally researched ${company} for 20 minutes before writing it. The subject line, the opening sentence, and the pain points must all be unmistakably specific to ${company} and ${firstname}'s situation. If ${firstname} reads this and thinks "this could have been sent to anyone," the email has failed.

IMPORTANT — DO NOT ASSUME HUBSPOT FAMILIARITY:
This contact may be evaluating HubSpot for the first time, may be comparing it to other platforms, or may be an existing user looking to get more value from it. You do NOT know which. Write every email so it works for all three scenarios. Do not assume they are already on HubSpot. Do not say "you're already using HubSpot." Frame TPG's value as: we help organizations in your industry figure out whether HubSpot is the right fit AND get the most out of it if they choose it. The pitch is about TPG's expertise in their industry and in HubSpot, not about optimizing something they may not have yet.

=== COMPANY INTELLIGENCE BRIEF ===
You have been given scraped data directly from ${company}'s website. Use this data aggressively. Reference specific things you found — their headline positioning, recent news, what they write about, how they describe themselves. This is the research Scott did before writing.

${companyIntelligenceBrief}
===================================

ABOUT THE PEDOWITZ GROUP:
TPG is a certified HubSpot partner and Revenue Marketing consultancy. We have worked with 1,300+ clients across industries including hospitality and sports organizations, and have generated over $25B in marketing-sourced revenue. We are not generalists. We specialize in HubSpot and we have the case studies in ${firstname}'s exact industry to prove it.

INDUSTRY PROFILE FOR THIS CONTACT:
- Industry: ${industryProfile.industryLabel}
- Who they are: ${industryProfile.audienceContext}
- HubSpot opportunity in this industry: ${industryProfile.hubspotAngle}
- Industry-specific pain points — pick the ONE most relevant to ${company} based on the intelligence brief above:
${industryProfile.painPoints.map((p, i) => `  ${i + 1}. ${p}`).join('\n')}
- Tone: ${industryProfile.toneGuidance}

HUBSPOT POSITIONING GUIDANCE:
- Do NOT assume ${firstname} is already using HubSpot. They may be evaluating it, comparing platforms, or looking for a reason to adopt it.
- Do NOT say "you're already on HubSpot" or "as a HubSpot user."
- DO position TPG as the firm that helps ${industryProfile.industryLabel} organizations both evaluate HubSpot and get full value from it once they choose it.
- Frame the email around the business problem first. HubSpot is the solution TPG brings, not an assumption about what they already have.
- Acceptable framings: "organizations like ${company} are using HubSpot to solve exactly this," "if you're evaluating how to solve [problem], HubSpot is what we use with clients in your space," "whether you're new to HubSpot or looking to get more from it, TPG has done this for teams like yours."

${intentBlock}

FEATURED HUBSPOT SERVICE FOR THIS EMAIL:
- Service: ${featuredService.label}
- URL: ${featuredService.url}
- Industry-specific pain: ${featuredService.pain}
- Angle: ${featuredService.angle}

PROSPECT DATA:
- Name: ${firstname}
- Title: ${jobtitle}
- Company: ${company}
- Industry Category: ${industry_category}
- Intent Topic Searched: ${intent_topic_searched || 'Not available'}
- Employee Count: ${numemployees}
- Annual Revenue: ${annualrevenue}
- Website: ${website}
- Web Technologies: ${web_technologies || 'Not listed'}
- Company Description: ${description || 'Not provided'}

BEHAVIORAL SIGNALS:
${BehavioralContext}

PRIOR EMAILS (DO NOT REPEAT ANYTHING FROM THESE):
${priorEmailsBlock}

=== SUBJECT LINE RULES ===
The subject line must feel like Scott wrote it after looking at ${company}'s website and knowing exactly who ${firstname} is.

GOOD subject lines reference something specific:
- Something from the company intelligence brief above
- The exact intent topic they searched
- A specific pain point for their type of organization
- Something timely about their company or industry

BAD subject lines are generic:
- "HubSpot tips for hotels"
- "Quick question for you"
- "Improving your marketing"

The subject line must be 8 words or fewer. No dashes or hyphens. Must be completely different from all prior subjects.

=== OPENING SENTENCE RULES ===
The opening sentence is the most important sentence in the email. It must contain a specific, researched observation about ${company} that proves Scott actually looked at their business before writing.

REQUIRED: Choose the STRONGEST available signal as your opening hook. Do not default to the intent topic every time. Rotate across signals across the sequence so each email opens differently.

Available hooks — use the best one for THIS email that has not been used in a prior email:

- COMPANY NEWS: "I saw that ${company} recently [specific announcement from news block]..." — USE THIS FIRST when fresh news is available. It is the most compelling opener.
- COMPANY POSITIONING: "I was on the ${company} site and noticed you [specific thing from homepage or about page]..." — Strong when their own words reveal a strategic priority.
- CONTENT / THOUGHT LEADERSHIP: "I saw ${company} has been publishing content around [specific topic from blog block] — which tells me [specific implication]..." — Use when their content reveals a clear focus area.
- BEHAVIORAL SIGNAL: "Noticed you spent some time on our [specific page] — that usually means [specific implication]..." — Use when page view data is meaningful.
- INTENT TOPIC: "When a ${industryProfile.industryLabel} organization is actively researching ${intent_topic_searched || 'solutions like this'}, it usually points to [specific business implication]..." — Valid hook but do NOT lead with this every email. Use it when no stronger company-specific signal is available, or rotate it in after other hooks have been used.

ROTATION RULE: Look at the prior emails block. If the last email opened with the intent topic, do NOT open with it again. Choose a different hook. Vary the opening signal across the sequence so the emails feel like ongoing research, not a template on repeat.

The opening sentence MUST mention ${company} by name OR reference something specific only ${company} would recognize.

BAD openers (NEVER USE):
- "I hope this finds you well"
- "I came across your profile"
- "As a leader in the ${industryProfile.industryLabel} industry..."
- "Many companies like yours..."
- "I wanted to reach out because..."

=== PAIN POINT PERSONALIZATION RULES ===
Do NOT use a generic industry pain point. Use the company intelligence brief to identify a pain that is specific to ${company}'s actual situation and stage.

For example:
- If their homepage talks about "growing their footprint" — pain is about scaling processes without adding headcount
- If their news mentions a new property or venue — pain is about onboarding new revenue streams quickly
- If their content is focused on guest or fan experience — pain is about connecting experience data to automated lifecycle journeys
- If they describe multiple locations or properties — pain is about unified data across all of them

Make the pain feel like Scott noticed something specific about ${company} and connected it to a gap he has seen at similar organizations.

=== NON-REPETITION RULES (HARD FAIL) ===
- Do NOT repeat any idea, framing, pain point, analogy, or opening style from ANY prior email
- Do NOT reuse sentence structure or paragraph structure from prior emails
- Every email must advance the conversation with a completely fresh angle
- If similarity to any prior email exceeds minimal level, the response is INVALID

=== REQUIRED LINKS (ALL MUST BE INCLUDED) ===

1. FEATURED SERVICE — ONE single-word hyperlink woven naturally into a sentence:
   <a href="${featuredService.url}" style="font-weight:bold;text-decoration:underline;color:#A2CF23;">word</a>

${includeCaseStudy
  ? `2. CASE STUDY — ONE naturally flowing reference to the ${caseStudyLabel}. Anchor text must fit the sentence — do not use "click here":
   <a href="${caseStudyUrl}" style="font-weight:bold;text-decoration:underline;color:#A2CF23;">anchor text that flows</a>
   Good example: "We built exactly this for <a href="${caseStudyUrl}" style="font-weight:bold;text-decoration:underline;color:#A2CF23;">one of the most recognized hospitality brands in the world</a>."`
  : `2. CASE STUDY — Do NOT include a case study link in email ${SEQUENCE_STEP}. Skip this entirely.`}

3. CALENDAR CTA — ONE short standalone paragraph with a meeting link:
   <a href="https://meetings.hubspot.com/scott-benedetti" style="font-weight:bold;text-decoration:underline;color:#A2CF23;">anchor text</a>
   Good example: "If this resonates, I'd love to <a href="https://meetings.hubspot.com/scott-benedetti" style="font-weight:bold;text-decoration:underline;color:#A2CF23;">grab 20 minutes</a> this week."

=== FORMATTING ===
- Subject: 8 words or fewer. No dashes.
- Salutation on its own line: "${firstname},"
- One blank line after salutation
- Body: 80-110 words. Tight paragraphs. No bullets. No signature.
- Each paragraph separated by ONE blank line
- HTML-safe. Use <a> tags only for links.
- NEVER use dashes, hyphens, or em dashes anywhere

OUTPUT FORMAT (exactly):
Subject: <subject>

Body:
<body>`;

  let subject = "", bodyText = "", attempt = 0;

  while (attempt < MAX_SUBJECT_RETRIES && !subject) {
    attempt++;

    const res = await axios.post(
      "https://api.anthropic.com/v1/messages",
      {
        model: "claude-sonnet-4-20250514",
        max_tokens: 1500,
        temperature: 0.7,
        system: `You are Scott Benedetti, Partner and EVP at The Pedowitz Group, writing a personal outbound email to ${firstname} at ${company}.

You have just spent time on ${company}'s website. You looked at their homepage, read their about page, browsed their news or blog, and formed a real point of view on their business before writing this email. That research shows in every sentence.

Writing style: Short. Direct. Peer-to-peer. Like a busy executive who respects the recipient's time. No marketing polish. No perfect parallelism. Occasional fragments are fine. Think "typed this between calls" not "drafted by a copywriter."

CRITICAL — DO NOT ASSUME HUBSPOT FAMILIARITY:
You do not know whether ${firstname} is already using HubSpot, evaluating it, or just beginning to look at it. Never say or imply they are already a HubSpot user. Lead with the business problem specific to ${company}. Then position HubSpot and TPG as the solution. Frame it as: TPG helps ${industryProfile.industryLabel} organizations solve this exact problem using HubSpot, whether they are brand new to the platform or looking to get more out of it.

The ${industryProfile.industryLabel} industry context matters. Use industry-native vocabulary naturally: ${industry_category.toLowerCase().includes('hotel') || industry_category.toLowerCase().includes('lodging')
  ? 'RevPAR, ADR, direct bookings, group blocks, pre-arrival sequences, post-stay journeys, OTA dependence, loyalty tiers, property management systems'
  : 'season ticket renewals, partial plans, group sales, sponsorship activation, fan lifetime value, corporate suites, ticketing platforms, sponsorship pipelines'}.

Every email must:
1. Open with something Scott actually found when he looked at ${company}. Not a generic industry observation. Something specific to THEM.
2. Connect what he found to a real business problem. Lead with the pain, not with HubSpot.
3. Introduce HubSpot and TPG as the solution naturally, without assuming they already use it.
4. Make the pain point feel inevitable for a company at ${company}'s stage.
5. Include all required hyperlinks naturally woven into the copy.
6. Stay under 110 words.
7. End with a low-friction meeting ask.

The goal: get ${firstname} to think "this person actually understands our business" and book time with Scott.`,
        messages: [{ role: "user", content: userContent }]
      },
      {
        headers: {
          "x-api-key": ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01",
          "content-type": "application/json"
        },
        timeout: 30000
      }
    );

    const text = res.data?.content?.find(p => p.type === "text")?.text || "";
    const subjectMatch = text.match(/^\s*Subject:\s*(.+)\s*$/mi);
    const bodyMatch = text.match(/^\s*Body:\s*([\s\S]+)$/mi) || text.match(/^\s*Subject:[\s\S]*?\n\n([\s\S]+)$/mi);
    subject  = subjectMatch ? subjectMatch[1].trim().replace(/<[^>]+>/g, '') : "";
    bodyText = bodyMatch ? bodyMatch[1].trim() : "";
  }

  if (!subject) throw new Error("Missing subject after retries");

  return {
    subject: removeDashes(subject),
    bodyText: removeSignature(removeDashes(bodyText))
  };
}

// =============================
// HUBSPOT WRITE-BACK
// Properties: industry_hubspot_nurture_subject_line_em1-10
//             industry_hubspot_nurture_em1-10
//             industry_hubspot_nurture_claude_text_em1-10
// =============================
async function writeResults(contactId, { subject, bodyText }, sequenceStep = 1) {
  const bodyHtml = bodyText
    .replace(/\r\n/g, "\n")
    .split(/\n{2,}/)
    .map(p => `<p style="margin:0 0 16px;">${p.replace(/\n/g, "<br>")}</p>`)
    .join("\n");

  await axios.patch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
    {
      properties: {
        [`industry_hubspot_nurture_subject_line_em${sequenceStep}`]: subject,
        [`industry_hubspot_nurture_em${sequenceStep}`]: bodyHtml,
        [`industry_hubspot_nurture_claude_text_em${sequenceStep}`]: bodyText
      }
    },
    {
      headers: {
        Authorization: `Bearer ${HUBSPOT_TOKEN}`,
        "Content-Type": "application/json"
      },
      timeout: 10000
    }
  );
}

// =============================
// STATUS UPDATE
// =============================
async function updateStatus(contactId, status) {
  try {
    await axios.patch(
      `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
      { properties: { ai_email_step_status: status } },
      {
        headers: {
          Authorization: `Bearer ${HUBSPOT_TOKEN}`,
          "Content-Type": "application/json"
        },
        timeout: 5000
      }
    );
  } catch (err) {
    console.error(`Status update failed for ${contactId}:`, err.message);
  }
}

// =============================
// SERVER STARTUP
// =============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Industry HubSpot Nurture — Scott Benedetti running on port ${PORT}`);
  console.log(`⚡ Audience: Hotel/Lodging & Sports Teams | Intent-driven personalization active`);
  console.log(`⚡ Concurrency: ${CONCURRENCY} | Interval: ${PROCESS_INTERVAL_MS}ms`);
});
