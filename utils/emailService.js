/**
 * Email Service - Using HTTP API (no SMTP required)
 * Supports: Brevo (Sendinblue) - Free 300 emails/day
 * Uses native fetch() - no extra dependencies needed
 */

const BREVO_API_KEY = process.env.BREVO_API_KEY;
const FROM_EMAIL = process.env.EMAIL_FROM || 'noreply@migx.app';
const FROM_NAME = 'MIGX Community';

/**
 * Send email using Brevo HTTP API
 */
async function sendEmail(to, subject, htmlContent) {
  if (!BREVO_API_KEY) {
    console.error('BREVO_API_KEY not set in environment');
    return { success: false, error: 'Email service not configured' };
  }

  try {
    const response = await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST',
      headers: {
        'accept': 'application/json',
        'api-key': BREVO_API_KEY,
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        sender: { name: FROM_NAME, email: FROM_EMAIL },
        to: [{ email: to }],
        subject: subject,
        htmlContent: htmlContent
      })
    });

    if (!response.ok) {
      const errorData = await response.json();
      console.error('Brevo API error:', errorData);
      return { success: false, error: errorData.message || 'Failed to send email' };
    }

    const result = await response.json();
    return { success: true, data: result };
  } catch (error) {
    console.error('Email send error:', error);
    return { success: false, error: error.message };
  }
}

/**
 * Generate OTP email HTML template
 */
function getOtpEmailHtml(username, otp, title = 'Account Verification') {
  return `
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f5f5f5;">
      <div style="background: linear-gradient(135deg, #082919 0%, #00936A 100%); padding: 30px; border-radius: 10px; text-align: center;">
        <h1 style="color: white; margin: 0;">MIGX Community</h1>
      </div>
      <div style="background-color: white; padding: 30px; border-radius: 10px; margin-top: 20px;">
        <h2 style="color: #00936A;">${title}</h2>
        <p>Hi ${username},</p>
        <p>Your verification code is:</p>
        <h1 style="letter-spacing: 8px; color: #00936A; text-align: center; font-size: 48px; margin: 30px 0;">${otp}</h1>
        <p style="color: #666;">This code will expire in <strong>5 minutes</strong>.</p>
        <p style="color: #666; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
          If you didn't request this code, please ignore this email.
        </p>
      </div>
    </div>
  `;
}

async function sendOtpEmail(email, otp, username) {
  const html = getOtpEmailHtml(username, otp, 'Account Verification');
  return sendEmail(email, 'Your MIGX Verification Code', html);
}

async function sendActivationEmail(email, username, token) {
  const baseUrl = process.env.APP_URL || 
    (process.env.REPLIT_DEV_DOMAIN ? `https://${process.env.REPLIT_DEV_DOMAIN}` : 'http://localhost:5000');
  const activationUrl = `${baseUrl}/api/auth/activate/${token}`;
  
  const html = `
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f5f5f5;">
      <div style="background: linear-gradient(135deg, #082919 0%, #00936A 100%); padding: 30px; border-radius: 10px; text-align: center;">
        <h1 style="color: white; margin: 0;">Welcome to MIGX!</h1>
      </div>
      <div style="background-color: white; padding: 30px; border-radius: 10px; margin-top: 20px;">
        <h2 style="color: #00936A;">Hi ${username}!</h2>
        <p>Thank you for joining the MIGX Community.</p>
        <p>Please click the button below to activate your account:</p>
        <div style="text-align: center; margin: 30px 0;">
          <a href="${activationUrl}" style="background-color: #00936A; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; display: inline-block; font-weight: bold;">Activate Account</a>
        </div>
        <p style="color: #666; font-size: 14px;">Or copy this link: <br/>${activationUrl}</p>
        <p style="color: #666; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
          This link will expire in <strong>24 hours</strong>.
        </p>
      </div>
    </div>
  `;
  
  return sendEmail(email, 'Activate Your MIGX Account', html);
}

async function sendPasswordChangeOtp(email, username, otp) {
  const html = getOtpEmailHtml(username, otp, 'Email Change Request');
  return sendEmail(email, 'MIGX Email Change Verification', html);
}

async function sendForgotPasswordOtp(email, username, otp) {
  const html = `
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f5f5f5;">
      <div style="background: linear-gradient(135deg, #082919 0%, #00936A 100%); padding: 30px; border-radius: 10px; text-align: center;">
        <h1 style="color: white; margin: 0;">Password Reset Request</h1>
      </div>
      <div style="background-color: white; padding: 30px; border-radius: 10px; margin-top: 20px;">
        <h2 style="color: #00936A;">Hi ${username},</h2>
        <p>You requested to reset your password.</p>
        <p>Your verification code is:</p>
        <h1 style="letter-spacing: 8px; color: #00936A; text-align: center; font-size: 48px; margin: 30px 0;">${otp}</h1>
        <p style="color: #666;">This code will expire in <strong>10 minutes</strong>.</p>
        <p style="color: #666; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
          If you didn't request this reset, please ignore this email or secure your account immediately.
        </p>
      </div>
    </div>
  `;
  
  return sendEmail(email, 'MIGX Password Reset Request', html);
}

module.exports = {
  sendOtpEmail,
  sendActivationEmail,
  sendPasswordChangeOtp,
  sendForgotPasswordOtp
};
