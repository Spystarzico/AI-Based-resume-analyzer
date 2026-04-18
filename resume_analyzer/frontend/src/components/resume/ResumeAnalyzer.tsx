import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Separator } from '@/components/ui/separator';
import {
  Upload, FileText, Search, TrendingUp, AlertCircle,
  CheckCircle, Sparkles, Briefcase, Code, Star, Lightbulb, Brain, Loader2
} from 'lucide-react';

interface AnalysisResult {
  overallScore: number;
  skills: string[];
  experience: { years: number; level: string; details: string[] };
  keywords: string[];
  suggestions: string[];
  strengths: string[];
  jobMatch?: { score: number; matchedSkills: string[]; missingSkills: string[]; summary: string } | null;
  aiSummary: string;
}

async function analyzeWithBackend(resumeText: string, jobDescription: string): Promise<AnalysisResult> {
  // STEP 1: Log what we're sending
  console.log("SENDING:", {
    resume_text: resumeText.substring(0, 50) + "...",
    job_description: jobDescription || "(empty)"
  });

  const httpResponse = await fetch('http://127.0.0.1:8001/analyze', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      resume_text: resumeText,
      job_description: jobDescription || ''
    })
  });

  if (!httpResponse.ok) throw new Error(`API error: ${httpResponse.status}`);

  const response = { data: await httpResponse.json() };
  console.log("FULL RESPONSE:", response.data);
  
  // Backend response can be nested as: { data: { data: {...} } } or { data: {...} }
  const backendData = response.data?.data?.data ?? response.data?.data ?? {};
  const rawExperience = backendData?.experience;

  const years =
    typeof rawExperience === 'object' && rawExperience !== null
      ? Number(rawExperience?.years ?? 0) || 0
      : Number(String(rawExperience ?? '').match(/(\d+)\+?\s*years?/i)?.[1] ?? 0) || 0;

  const level =
    typeof rawExperience === 'object' && rawExperience !== null
      ? rawExperience?.level ?? 'Unknown'
      : 'Unknown';

  const experienceDetails =
    typeof rawExperience === 'object' && rawExperience !== null
      ? (Array.isArray(rawExperience?.details) ? rawExperience.details : [])
      : (typeof rawExperience === 'string' && rawExperience.trim() ? [rawExperience] : []);

  // Education field removed from frontend display
  
  // STEP 5: Extract match_score from backend response
  console.log("RESPONSE match_score:", backendData?.match_score);
  console.log("RESPONSE matched_skills:", backendData?.matched_skills);
  console.log("RESPONSE missing_skills:", backendData?.missing_skills);
  
  const jobMatch = backendData?.match_score
    ? {
        score: Number(backendData.match_score) || 0,
        matchedSkills: Array.isArray(backendData?.matched_skills) ? backendData.matched_skills : [],
        missingSkills: Array.isArray(backendData?.missing_skills) ? backendData.missing_skills : [],
        summary: `Job match score: ${backendData.match_score}% based on keyword overlap with job description`
      }
    : null;
  
  // Map backend response to frontend interface
  return {
    overallScore: backendData.overallScore || 75,
    skills: backendData.skills || [],
    experience: {
      years,
      level,
      details: experienceDetails,
    },
    keywords: backendData.skills || [], // Use skills as keywords
    strengths: backendData.strengths || [],
    suggestions: backendData.suggestions || [],
    aiSummary: backendData.summary || 'Resume analysis completed.',
    jobMatch: jobMatch
  } as AnalysisResult;
}

export default function ResumeAnalyzer() {
  const [resumeText, setResumeText] = useState('');
  const [jobDescription, setJobDescription] = useState('');
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'input' | 'results'>('input');
  const [loadingStep, setLoadingStep] = useState('');

  const sampleResume = `John Doe
Software Engineer | john.doe@email.com | (555) 123-4567

SUMMARY
Experienced Software Engineer with 5+ years in full-stack development, specializing in Python, React, and cloud technologies.

SKILLS
Programming: Python, JavaScript, TypeScript, Java, SQL
Frontend: React, Vue.js, HTML5, CSS3, Tailwind CSS
Backend: Django, Flask, Node.js, REST APIs
Cloud: AWS (EC2, S3, Lambda), Docker, Kubernetes
Databases: PostgreSQL, MongoDB, Redis
Tools: Git, Jenkins, Airflow, Spark

EXPERIENCE
Senior Software Engineer | TechCorp | 2021 - Present
• Led development of microservices architecture serving 1M+ users
• Implemented CI/CD pipelines reducing deployment time by 60%
• Technologies: Python, React, AWS, Kubernetes

Software Engineer | DataSystems Inc | 2019 - 2021
• Built ETL pipelines processing 10TB+ daily data
• Optimized database queries improving performance by 40%
• Technologies: Python, Spark, PostgreSQL, Airflow

EDUCATION
B.S. Computer Science | University of Technology | 2015-2019 | GPA: 3.8/4.0

CERTIFICATIONS
AWS Certified Solutions Architect | Google Cloud Professional Data Engineer`;

  const sampleJD = `Senior Data Engineer
Requirements:
• 4+ years of experience in data engineering
• Strong proficiency in Python and SQL
• Experience with Apache Spark and Airflow
• Knowledge of cloud platforms (AWS/GCP/Azure)
• Experience with data warehousing (BigQuery/Redshift/Snowflake)
• Familiarity with Kafka/Kinesis streaming technologies`;

  const handleAnalyze = async () => {
    if (!resumeText.trim()) return;
    setIsAnalyzing(true);
    setError(null);
    setResult(null);

    try {
      setLoadingStep('Sending resume to Gemini AI...');
      await new Promise(r => setTimeout(r, 500));
      setLoadingStep('Extracting skills, experience and education...');
      await new Promise(r => setTimeout(r, 300));
      setLoadingStep('Generating insights and recommendations...');

      const analysis = await analyzeWithBackend(resumeText, jobDescription);
      setResult(analysis);
      setActiveTab('results');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Analysis failed. Please try again.');
    } finally {
      setIsAnalyzing(false);
      setLoadingStep('');
    }
  };

  const scoreColor = (s: number) => s >= 80 ? 'text-green-600' : s >= 60 ? 'text-yellow-600' : 'text-red-600';
  const scoreStroke = (s: number) => s >= 80 ? '#22c55e' : s >= 60 ? '#eab308' : '#ef4444';

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h2 className="text-2xl font-bold text-slate-900 flex items-center gap-2">
            <Brain className="h-7 w-7 text-blue-600" />
            AI Resume Analyzer
          </h2>
          <p className="text-slate-500 mt-1 text-sm flex items-center gap-1">
            <Sparkles className="h-3.5 w-3.5 text-indigo-500" />
            Powered by Gemini AI — real intelligence, not keyword matching
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={() => { setResumeText(sampleResume); setJobDescription(sampleJD); }}>
            Load Sample
          </Button>
          {result && (
            <Button variant="outline" size="sm" onClick={() => { setResult(null); setActiveTab('input'); }}>
              New Analysis
            </Button>
          )}
        </div>
      </div>

      {/* ── INPUT ── */}
      {activeTab === 'input' && (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <FileText className="h-5 w-5 text-blue-600" /> Resume
                </CardTitle>
                <CardDescription>Paste your resume text below</CardDescription>
              </CardHeader>
              <CardContent>
                <Textarea value={resumeText} onChange={e => setResumeText(e.target.value)}
                  placeholder="Paste your resume here..." className="min-h-[320px] font-mono text-sm" />
                <p className="text-xs text-slate-400 mt-2 flex items-center gap-1">
                  <Upload className="h-3 w-3" />
                  {resumeText.length > 0 ? `${resumeText.length} characters` : 'Paste plain text from your PDF/DOCX'}
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Briefcase className="h-5 w-5 text-indigo-600" /> Job Description
                  <Badge variant="outline" className="text-xs font-normal">Optional</Badge>
                </CardTitle>
                <CardDescription>Paste for job match scoring</CardDescription>
              </CardHeader>
              <CardContent>
                <Textarea value={jobDescription} onChange={e => setJobDescription(e.target.value)}
                  placeholder="Paste job description here..." className="min-h-[320px] font-mono text-sm" />
                <p className="text-xs text-slate-400 mt-2">
                  {jobDescription.length > 0 ? `${jobDescription.length} characters` : 'Leave empty for general resume analysis'}
                </p>
              </CardContent>
            </Card>
          </div>

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
              <AlertCircle className="h-5 w-5 text-red-500 shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-red-700">Analysis Failed</p>
                <p className="text-sm text-red-600 mt-1">{error}</p>
              </div>
            </div>
          )}

          <Button className="w-full h-12 text-base" onClick={handleAnalyze} disabled={!resumeText.trim() || isAnalyzing}>
            {isAnalyzing
              ? <span className="flex items-center gap-2"><Loader2 className="h-5 w-5 animate-spin" />{loadingStep || 'Analyzing...'}</span>
              : <span className="flex items-center gap-2"><Sparkles className="h-5 w-5" />Analyze with AI</span>}
          </Button>
        </>
      )}

      {/* ── RESULTS ── */}
      {activeTab === 'results' && result && (
        <div className="space-y-6">

          {/* AI Summary */}
          <Card className="border-blue-200 bg-blue-50">
            <CardContent className="p-5 flex items-start gap-3">
              <Brain className="h-5 w-5 text-blue-600 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-semibold text-blue-800 mb-1">Gemini AI Summary</p>
                <p className="text-sm text-blue-700">{result.aiSummary}</p>
              </div>
            </CardContent>
          </Card>

          {/* Scores */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card className="border-2 border-slate-100">
              <CardContent className="p-6">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-slate-500 mb-1">Overall Resume Score</p>
                    <p className={`text-5xl font-bold ${scoreColor(result.overallScore)}`}>{result.overallScore}%</p>
                    <p className="text-sm text-slate-500 mt-2">{result.experience.level || 'Unknown'}</p>
                  </div>
                  <div className="w-28 h-28 relative">
                    <svg className="w-full h-full -rotate-90" viewBox="0 0 120 120">
                      <circle cx="60" cy="60" r="50" stroke="#e2e8f0" strokeWidth="12" fill="none" />
                      <circle cx="60" cy="60" r="50" stroke={scoreStroke(result.overallScore)}
                        strokeWidth="12" fill="none"
                        strokeDasharray={`${result.overallScore * 3.14} 314`} strokeLinecap="round" />
                    </svg>
                    <span className={`absolute inset-0 flex items-center justify-center text-lg font-bold ${scoreColor(result.overallScore)}`}>
                      {result.overallScore}
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>

            {jobDescription.trim().length > 0 && result.jobMatch ? (
              <Card className="border-2 border-slate-100">
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Star className="h-4 w-4 text-yellow-500" /> Job Match Score
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="flex items-end gap-3 mb-3">
                    <p className={`text-4xl font-bold ${scoreColor(result.jobMatch.score)}`}>{result.jobMatch.score}%</p>
                    <Progress value={result.jobMatch.score} className="flex-1 mb-2" />
                  </div>
                  <p className="text-xs text-slate-500">{result.jobMatch.summary}</p>
                </CardContent>
              </Card>
            ) : (
              <Card className="border-2 border-dashed border-slate-200 flex items-center justify-center">
                <CardContent className="text-center p-6">
                  <Briefcase className="h-8 w-8 text-slate-300 mx-auto mb-2" />
                  <p className="text-sm text-slate-400">Add a job description to see match score</p>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Skills + Experience */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Code className="h-5 w-5 text-purple-600" /> Skills Detected
                  <Badge variant="secondary">{result.skills.length}</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {result.skills.map((s, i) => <Badge key={i} variant="secondary" className="text-sm">{s}</Badge>)}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <TrendingUp className="h-5 w-5 text-green-600" /> Experience
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex justify-between">
                  <span className="text-slate-500">Years</span>
                  <span className="font-semibold">{(result?.experience?.years ?? 0)}+</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-500">Level</span>
                  <Badge>{result?.experience?.level || 'Unknown'}</Badge>
                </div>
                <Separator />
                <ul className="space-y-1.5">
                  {(result?.experience?.details ?? []).map((d, i) => (
                    <li key={i} className="text-sm text-slate-600 flex items-start gap-2">
                      <CheckCircle className="h-3.5 w-3.5 text-green-500 shrink-0 mt-0.5" />{d}
                    </li>
                  ))}
                </ul>
              </CardContent>
            </Card>
          </div>

          {/* Job Match Detail */}
          {jobDescription.trim().length > 0 && result.jobMatch && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Star className="h-5 w-5 text-yellow-500" /> Job Match Breakdown
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div>
                    <p className="text-sm font-medium text-green-600 mb-2">✅ Matched Skills ({result.jobMatch.matchedSkills.length})</p>
                    <div className="flex flex-wrap gap-1.5">
                      {result.jobMatch.matchedSkills.map((s, i) => (
                        <Badge key={i} variant="outline" className="bg-green-50 text-green-700 border-green-200">
                          <CheckCircle className="h-3 w-3 mr-1" />{s}
                        </Badge>
                      ))}
                    </div>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-orange-600 mb-2">⚠️ Missing Skills ({result.jobMatch.missingSkills.length})</p>
                    <div className="flex flex-wrap gap-1.5">
                      {result.jobMatch.missingSkills.map((s, i) => (
                        <Badge key={i} variant="outline" className="bg-orange-50 text-orange-700 border-orange-200">
                          <AlertCircle className="h-3 w-3 mr-1" />{s}
                        </Badge>
                      ))}
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Strengths + Suggestions */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-green-600" /> Key Strengths
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2">
                  {result.strengths.map((s, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-slate-700">
                      <span className="bg-green-100 text-green-700 rounded-full w-5 h-5 flex items-center justify-center shrink-0 text-xs font-bold">{i + 1}</span>
                      {s}
                    </li>
                  ))}
                </ul>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Lightbulb className="h-5 w-5 text-amber-500" /> Improvement Suggestions
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2">
                  {result.suggestions.map((s, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-slate-700">
                      <span className="bg-amber-100 text-amber-700 rounded-full w-5 h-5 flex items-center justify-center shrink-0 text-xs font-bold">{i + 1}</span>
                      {s}
                    </li>
                  ))}
                </ul>
              </CardContent>
            </Card>
          </div>

          {/* Keywords Section */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Search className="h-5 w-5 text-slate-500" /> Key Resume Keywords
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {result.keywords.map((k, i) => <Badge key={i} variant="outline" className="text-sm">{k}</Badge>)}
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
